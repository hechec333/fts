package disk

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fts/internal/common"
	"fts/internal/types"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
)

var muti = 1024
var meta = "ddm.meta"
var max = 1024 * 1024 * 128 //128MB

type docchunkInfo struct {
	Mapping map[int64]int64 // ID对应文档在文件的偏移值
	Lengths map[int64]int   //ID对应的文档对象大小，允许变长编码
	Lens    int64
}

func (ci *docchunkInfo) Len() int64 {

	return ci.Lens
}

type WriteHandle struct {
	offset int64
	b      []byte
}
type SequenceHandle struct {
	f      *os.File
	fflush chan struct{}
	stop   chan struct{}
	ch     chan WriteHandle
}

func NewSequenceHandle(f *os.File) *SequenceHandle {
	s := &SequenceHandle{
		f:      f,
		fflush: make(chan struct{}),
		stop:   make(chan struct{}),
		ch:     make(chan WriteHandle, 16),
	}

	go sequenceWriteFile(s.f, s.ch, s.fflush, s.stop)

	return s
}
func sequenceWriteFile(
	f *os.File,
	ch chan WriteHandle,
	fflush chan struct{},
	stop chan struct{},
) {
	var (
		batch        = make([]byte, 0, 1024*1024*32)
		offset int64 = -1
		//apply        = make([]int64, 0)
	)
	for {
		select {
		case <-fflush:
			if offset != -1 {
				f.WriteAt(batch, offset)
			}
			f.Sync()
			offset = -1
		case <-stop:
			return
		case b, ok := <-ch:
			if !ok {
				f.Sync()
				return
			}
			if offset == -1 {
				offset = b.offset
				//apply = append(apply, offset)
			}
			batch = append(batch, b.b...)
			if len(batch) >= 1024*1024*32 {
				f.WriteAt(batch, offset)
				common.DINFO("Writing Into %v 32MB", f.Name())
				offset = -1
				batch = batch[0:0] //将切片清零，但是保留容量
			}
		}
	}
}
func (seq *SequenceHandle) Go(offset int64, b []byte) {
	seq.ch <- WriteHandle{
		offset: offset,
		b:      b,
	}
}

func (seq *SequenceHandle) Close() {
	seq.stop <- struct{}{}
}

func (seq *SequenceHandle) Flush() {
	seq.fflush <- struct{}{}
}

type DocDiskManager struct {
	mu       sync.RWMutex
	loadmaps map[string][]string //记录某次load的reflects文档对应生成的磁盘文件，key是文档名
	ids      map[string][]int64  //记录某个磁盘文件对应有多少个id对应,倒排索引
	chunks   map[string]*docchunkInfo
	reflects map[string]reflect.Type //无法序列化reflect.Type
	sequence map[string]*SequenceHandle
	last     *SequenceHandle
	root     string
	max      int64
}

func NewDocDiskManager(root string) *DocDiskManager {
	ddm := &DocDiskManager{
		loadmaps: make(map[string][]string),
		ids:      make(map[string][]int64),
		chunks:   make(map[string]*docchunkInfo),
		reflects: make(map[string]reflect.Type),
		sequence: make(map[string]*SequenceHandle),
		root:     root,
		last:     nil,
		max:      int64(max),
	}
	ddm.loadMeta()
	return ddm
}
func (ddm *DocDiskManager) meta() string {
	return "hash_doc.meta"
}
func (ddm *DocDiskManager) loadMeta() {
	ddm.mu.Lock()
	defer ddm.mu.Unlock()
	path := ddm.root + "/" + ddm.meta()
	if common.IsExist(path) {
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		dec := gob.NewDecoder(f)
		dec.Decode(&ddm.loadmaps)
		dec.Decode(&ddm.ids)
		dec.Decode(&ddm.chunks)
		err = dec.Decode(&ddm.reflects)
		if err != nil {
			panic(err)
		}
	}
}
func (ddm *DocDiskManager) persite() {
	ddm.mu.RLock()
	defer ddm.mu.RUnlock()
	var file *os.File
	path := ddm.root + "/" + ddm.meta()
	if !common.IsExist(path) {
		file, _ = os.Create(path)
	} else {
		file, _ = os.Open(path)
	}

	enc := gob.NewEncoder(file)

	enc.Encode(&ddm.loadmaps)
	enc.Encode(&ddm.ids)
	enc.Encode(&ddm.chunks)
	//
	refl := make(map[string][]byte)
	for k, v := range ddm.reflects {
		jsc, _ := json.Marshal(v)
		refl[k] = jsc
	}
	enc.Encode(refl)
}
func (ddm *DocDiskManager) SaveMeta() {
	ddm.persite()
}
func (ddm *DocDiskManager) docLen(doc types.Document) (i int64) {
	ddm.mu.RLock()
	defer ddm.mu.RUnlock()
	arr, ok := ddm.loadmaps[common.ExtractMetaTypeName(reflect.TypeOf(doc))]
	if !ok {
		i = -1
		return
	}
	for _, v := range arr {
		ck := ddm.chunks[v]
		i += int64(ck.Len())
	}
	return
}
func (ddm *DocDiskManager) getNextChunkName(sha256 string) string {
	return ddm.root + "/" + sha256 + "-" + strconv.Itoa(len(ddm.chunks)) + ".xck"
}

// add file <-> id ,binary insert
func (ddm *DocDiskManager) addID(path string, id int64) {

	ids := ddm.ids[path]

	idx := len(ids) - 1
	for ; idx >= 0; idx-- {
		if ids[idx] < id {
			break
		}
	}

	if idx == len(ids)-1 {
		ids = append(ids, id)
	} else if idx == -1 {
		ids = append([]int64{id}, ids...)
	} else {
		ids = append(ids[:idx], append([]int64{id}, ids[idx+1:]...)...)
	}

	ddm.ids[path] = ids
}
func (ddm *DocDiskManager) goLookID(id int64) (string, bool) {
	var (
		proc = 4
		sl   = make([]string, 0, 8)
	)

	for k := range ddm.ids {
		sl = append(sl, k)
	}
	success := make(chan string, 1)
	stop := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < proc; i++ {
		go func(seq int) {
			defer func() {
				stop <- struct{}{}
			}()
			for idx := seq; idx < len(ddm.ids); idx += proc {
				ids := ddm.ids[sl[idx]]
				select {
				case <-ctx.Done():
					return
				default:
				}
				if common.BinarySearch(ids, id) {
					success <- sl[idx]
					return
				}
			}
		}(i)
	}
	cnt := 0
	defer cancel() // notify all goroutine return
	for {
		select {
		case s := <-success:
			return s, true
		case <-stop:
			cnt++
			if cnt == proc {
				return "", false
			}
		}
	}

}

// look id <-> file, binary search
func (ddm *DocDiskManager) lookID(id int64) (string, bool) {
	if int64(ddm.max) > 1024*1024*64 && len(ddm.ids) > 8 {
		return ddm.goLookID(id)
	}
	for k, v := range ddm.ids {
		if common.BinarySearch(v, id) {
			return k, true
		}
	}
	return "", false
}
func (ddm *DocDiskManager) addDoc(doc types.Document) {
	ddm.mu.Lock()
	defer ddm.mu.Unlock()
	t := reflect.TypeOf(doc)
	s := common.ExtractMetaTypeName(t)
	if _, ok := ddm.reflects[s]; !ok {
		ddm.reflects[s] = t
	}

	var (
		name string
		ckc  *docchunkInfo
		meta string
	)

	if p := path.Base(s); p[0] == '*' {
		meta = p[1:]
	} else {
		meta = p
	}
	for n, ck := range ddm.chunks {
		if ck.Len() < ddm.max {
			name = n
			ckc = ck
			break
		}
	}
	if name != "" {
		offset := ckc.Len()
		ID := doc.UUID()
		bytes := doc.Serial()

		seq := ddm.sequence[name]
		seq.Go(offset, bytes)
		ckc.Lengths[ID] = len(bytes)
		ckc.Lens += int64(len(bytes))
		ckc.Mapping[ID] = offset
		//ddm.ids[name] = append(ddm.ids[name], ID)
		ddm.addID(name, ID)
		return
	}
	name = ddm.getNextChunkName(meta)
	ID := doc.UUID()
	bytes := doc.Serial()
	ddm.chunks[name] = &docchunkInfo{
		Mapping: map[int64]int64{
			ID: 0,
		},
		Lengths: map[int64]int{
			ID: len(bytes),
		},
		Lens: int64(len(bytes)),
	}
	ddm.loadmaps[meta] = append(ddm.loadmaps[meta], name)
	//ddm.ids[name] = append(ddm.ids[name], ID)
	ddm.addID(name, ID)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		common.DFAIL("Create %v Error %v", name, err)
		return
	}
	if ddm.last != nil {
		ddm.last.Flush()
	}
	seq := NewSequenceHandle(f)
	ddm.sequence[name] = seq
	seq.Go(0, bytes)

	ddm.last = seq
}
func (ddm *DocDiskManager) getDocTypeInfo(path string) reflect.Type {
	for k, v := range ddm.loadmaps {
		for _, vv := range v {
			if vv == path {
				return ddm.reflects[k]
			}
		}
	}
	return nil
}

func (ddm *DocDiskManager) goGetDoc(nums int, key int64) types.Document {
	lens := len(ddm.ids)
	ck := lens / nums
	success := make(chan string)
	complte := make(chan struct{})
	slice := make([]string, lens)
	j := 0
	for k := range ddm.ids {
		slice[j] = k
		j++
	}

	for i := 0; i < nums; i++ {
		go func(seq int) {
			up := common.Min((seq+1)*lens/nums, lens)
			for z := ck * seq; z < up; z++ {
				for _, id := range ddm.ids[slice[z]] {
					if id == key {
						success <- slice[z]
						goto re
					}
				}
			}
		re:
			complte <- struct{}{}
		}(i)
	}
	var path string
	count := 0
	for {
		select {
		case path = <-success:
			goto l
		case <-complte:
			count++
			if count == nums && path == "" {
				return nil
			}
		}
	}
l:
	ckc := ddm.chunks[path]
	offset := ckc.Mapping[key]
	length := ckc.Lengths[key]
	f, _ := os.Open(path)
	buf := make([]byte, length)
	f.ReadAt(buf, offset)

	ty := ddm.getDocTypeInfo(path)
	doc := reflect.New(ty).Interface().(types.Document)
	doc.Dump(buf)
	return doc
}

func (ddm *DocDiskManager) getDoc(key int64) types.Document {
	ddm.mu.RLock()
	defer ddm.mu.RUnlock()
	//var path string
	// if len(ddm.ids) > muti {
	// 	// muti-goroutine search
	// 	return ddm.goGetDoc(4, key)
	// }
	// for p, ids := range ddm.ids {
	// 	for _, id := range ids {
	// 		if id == key {
	// 			path = p
	// 			goto l
	// 		}
	// 	}
	// }
	var (
		path string
		ok   bool
	)
	path, ok = ddm.lookID(key)

	if path == "" || !ok {
		return nil
	}

	ckc := ddm.chunks[path]
	off := ckc.Mapping[key]
	lens := ckc.Lengths[key]

	f, _ := os.Open(path)

	buf := make([]byte, lens)
	ty := ddm.getDocTypeInfo(path)
	//doc := reflect.New(ddm.reflects[])
	f.ReadAt(buf, off)
	doc := reflect.New(ty).Interface().(types.Document)
	doc.Dump(buf)
	return doc
}

func (ddm *DocDiskManager) GetDoc(uuid int64) types.Document {
	//xid := strconv.FormatInt(uuid, 10)
	return ddm.getDoc(uuid)
}
func (ddm *DocDiskManager) AddDoc(doc types.Document) {
	ddm.addDoc(doc)
}

func (ddm *DocDiskManager) Docs(doc types.Document) int64 {
	return ddm.docLen(doc)
}

func (ddm *DocDiskManager) EnumDocsID(doc types.Document, size int) chan int64 {
	ch := make(chan int64, size)
	name := common.ExtractMetaTypeName(reflect.TypeOf(doc))
	go func() {
		v, ok := ddm.loadmaps[name]
		if ok {
			for _, vv := range v {
				ids := ddm.ids[vv]
				for _, rv := range ids {
					ch <- rv
				}
			}
		}
		close(ch)
	}()

	return ch
}

func (ddm *DocDiskManager) EnumDocTypes() []types.Document {
	tys := []types.Document{}
	for _, v := range ddm.reflects {
		tys = append(tys, reflect.New(v).Interface().(types.Document))
	}
	return tys
}

func (ddm *DocDiskManager) Flush() {
	for _, v := range ddm.sequence {
		v.Flush()
	}
}
