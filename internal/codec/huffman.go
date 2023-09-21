package codec

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"fts/internal/common"
	"io"
)

type HuffmanDiskCodec struct {
	r io.Reader
	w io.Writer
}

func (h *HuffmanDiskCodec) BindR(r io.Reader) {
	h.r = r
}
func (h *HuffmanDiskCodec) BindW(w io.Writer) {
	h.w = w
}

func (h *HuffmanDiskCodec) Encode(b []byte) (int64, error) {
	m := countBytes(b)
	hf := huffmanTree{}
	mm, w := split(m)
	err := hf.build(len(m), mm, w)
	if err != nil {
		return -1, err
	}
	hf.coding(m)

	bs := hf.encodeHeader()
	xs, err := hf.encodeBody(b)
	if err != nil {
		return -1, err
	}
	bs = append(bs, xs...)

	i64, err := h.w.Write(bs)

	return int64(i64), err
}

func (h *HuffmanDiskCodec) Decode() ([]byte, error) {
	b, err := io.ReadAll(h.r)
	if err != nil {
		return nil, err
	}
	hf := huffmanTree{}
	idx, header, err := hf.decodeHeader(b)
	if err != nil {
		return nil, err
	}
	return hf.decodeBody(header, b[idx:])
}

type huffmanNode struct {
	weight                int
	parent, lchild, rchid int
}

type huffmanTree struct {
	root     []*huffmanNode
	maps     map[int]byte
	reflects map[byte]string
	lens     int
}

type huffmanHeader struct {
	checkSum int
	root     []*huffmanNode //哈夫曼树
	reflects map[byte]byte  //key 待编码，value 编码结果
	bytes    map[byte]uint8 // value 编码结果位数
}

func countBytes(b []byte) map[byte]int {
	s := make(map[byte]int)
	for i := 0; i < len(b); i++ {
		_, ok := s[b[i]]
		if !ok {
			s[b[i]] = 1
		}
		s[b[i]]++
	}
	return s
}
func split(s map[byte]int) (b []byte, w []int) {
	for k, v := range s {
		b = append(b, k)
		w = append(w, v)
	}
	return
}
func (h *huffmanTree) build(n int, m []byte, w []int) error {
	if n <= 1 {
		return errors.New("n <= 1")
	}
	lens := 2*n - 1
	h.root = make([]*huffmanNode, lens+1)

	for i := 1; i <= lens; i++ {
		h.root[i].lchild = 0
		h.root[i].parent = 0
		h.root[i].rchid = 0
	}

	for i := 1; i <= n; i++ {
		h.root[i].weight = w[i-1]
		h.maps[i] = m[i-1]
	}

	for i := n + 1; i <= lens; i++ {
		s1, s2 := h.choose(i - 1)
		h.root[i].lchild = s1
		h.root[i].rchid = s2
		h.root[i].weight = h.root[s1].weight + h.root[s2].weight
		h.root[s1].parent = i
		h.root[s2].parent = i
	}
	h.lens = lens + 1
	return nil
}

func (h *huffmanTree) choose(n int) (int, int) {
	var min1, min2 int
	var idx1, idx2 int
	for i := 1; i <= n; i++ {
		if h.root[i].parent == 0 {
			continue
		}
		t := h.root[i].weight

		if t < min2 {
			if t > min1 {
				min2 = t
				idx2 = i
			} else {
				min1 = t
				min2 = t
				idx1 = i
				idx2 = i
			}
		}
	}

	return idx1, idx2
}

// 'a' -> 0b110
func (h *huffmanTree) coding(s map[byte]int) {
	m := make(map[byte]string, 0)
	n := len(s)
	for i := 1; i <= n; i++ {
		f := h.root[i].parent
		c := i
		var b string
		dep := 0
		for f != 0 {
			if h.root[i].lchild == c {
				// 左子树编1
				b += "1" //encode(b, true, dep)
			} else {
				//右子树编0
				b = "0" //encode(b, false, dep)
			}
			dep++
			f = h.root[f].parent
		}
		m[byte(h.maps[i])] = b
	}
	h.reflects = m
}
func (h *huffmanTree) encodeBody(b []byte) ([]byte, error) {
	bbs := make([]byte, 1)
	pos := int64(0)
	for _, v := range b {
		bs, ok := h.reflects[v]
		if !ok {
			return nil, fmt.Errorf("not found meta data of %v", v)
		}
		if len(bbs)+1 == cap(bbs) {
			bbs = append(bbs, 0)[:len(bbs)] //扩容
		}
		pos = encodePos(bbs, sbyte(bs), pos, slen(bs))
	}

	return bbs, nil
}

func (h *huffmanTree) header() huffmanHeader {
	hh := huffmanHeader{
		root: h.root,
	}
	for k, v := range h.reflects {
		hh.bytes[k] = slen(v)
		hh.reflects[k] = sbyte(v)
	}
	return hh
}
func (h *huffmanTree) decodeHeader(b []byte) (int64, *huffmanHeader, error) {
	creader := NewCountReader(bytes.NewReader(b))
	d := gob.NewDecoder(creader)
	hh := huffmanHeader{}
	d.Decode(&hh.reflects)
	d.Decode(&hh.bytes)
	d.Decode(&hh.root)
	sl := creader.count
	crc32 := common.GetCrc32(b[:sl])
	d.Decode(&hh.checkSum)
	if hh.checkSum != int(crc32) {
		return -1, nil, fmt.Errorf("data broken,crc32 not equal")
	}
	return creader.count, &hh, nil
}
func (h *huffmanTree) encodeHeader() []byte {
	buf := new(bytes.Buffer)

	header := h.header()
	d := gob.NewEncoder(buf)
	d.Encode(&header.reflects)
	d.Encode(&header.bytes)
	d.Encode(&header.root)
	header.checkSum = int(common.GetCrc32(buf.Bytes()))
	d.Encode(&header.checkSum)
	return buf.Bytes()
}

func (h *huffmanTree) decodeBody(hh *huffmanHeader, b []byte) ([]byte, error) {
	pos := 0
	bbs := make([]byte, 1)
	decodelen := 0
	var ok bool
	for pos < 8*len(b) {
		isleaf := false
		child := len(hh.root)
		lens := 0
		var bs byte
		for !isleaf {
			start := pos / 8
			offset := pos % 8
			bit := getPos(b[start], uint8(offset))
			if bit {
				child = hh.root[child].lchild
			} else {
				child = hh.root[child].rchid
			}
			bs = encode(bs, bit, lens)
			lens++
			if hh.root[child].lchild == 0 || hh.root[child].rchid == 0 {
				//isleaf = true
				break
			}
			pos++
		}

		if len(bbs) == cap(bbs) {
			bbs = append(bbs, 0)[:len(bbs)]
		}
		bbs[decodelen], ok = decode(hh, bs, uint8(lens))
		if !ok {
			return nil, fmt.Errorf("decoding body error")
		}
		decodelen++
	}

	return bbs, nil
}

func encode(b byte, i bool, dep int) byte {
	maps := map[int]byte{
		0: 0xFE, 1: 0xFD, 2: 0xFB, 3: 0xF7, 4: 0xEF, 5: 0xDF, 6: 0xBF, 7: 0x7F,
	}
	if i {
		return b | 0xff - maps[dep]
	} else {
		return b & maps[dep]
	}
}

func decode(hf *huffmanHeader, b byte, len uint8) (byte, bool) {
	bs := make(map[uint8]byte)
	for k, v := range hf.reflects {
		if v == b {
			bs[hf.bytes[k]] = k
		}
	}
	bx, ok := bs[len]
	return bx, ok
}

func encodePos(b []byte, r byte, pos int64, len uint8) int64 {
	start := pos / 8
	off := uint8(pos % 8)
	//left := 8 - off
	bs := b[start]
	var s byte
	rea := getLow(r, len)
	rea = rea << off
	s = bs & rea
	b[start] = s
	if off+len > 8 {
		right := off + len - 8
		rea = getHight(r, right)
		b[start+1] = rea
	}
	return pos + int64(len)
}

func getLow(b byte, len uint8) byte {
	return b & ((1 << len) - 1)
}
func getHight(b byte, len uint8) byte {
	return b >> (8 - len)
}

// 0<=pos<8 true ==> 1
func getPos(b byte, pos uint8) bool {
	return (b>>pos)&1 == 1
}
func slen(s string) (z uint8) {
	for range s {
		z++
	}
	return
}
func sbyte(s string) (b byte) {
	for idx, r := range []byte(s) {
		if r == 0x01 {
			b = encode(b, true, idx)
		} else {
			b = encode(b, false, idx)
		}
	}
	return
}
