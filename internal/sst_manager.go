package internal

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fts/internal/common"
	"io"
	"os"
	"sort"
	"unsafe"
)

var (
	CACHE_NODE int64 = 4
)

type SstManager struct {
	lastid    int64
	nums      int
	f         *os.File
	root      string
	ca        list.List
	headers   []PageHeader
	locations []int64
}

func NewSSTM(root string) *SstManager {
	sstm := &SstManager{
		ca:   *list.New(),
		root: root,
	}

	sstm.init()

	return sstm
}

func (sm *SstManager) Search(s string) (*Record, bool) {
	cnt := 0
retry:
	for iter := sm.ca.Front(); iter != nil; iter = iter.Next() {
		xv := iter.Value.(*SortSuffixTable)

		if re := xv.SearchRecord(s); re != nil {
			return re, true
		}
	}
	cnt++
	sm.getPage(s)
	if cnt >= 2 {
		sm.flushAll()
	}
	goto retry
}

func (sm *SstManager) Insert(re Record) {

	var (
		err error
	)

	for iter := sm.ca.Front(); iter != nil; iter = iter.Next() {
		xv := iter.Value.(*SortSuffixTable)

		if err = xv.InsertRecord(re); err != nil {
			if err == ErrNoSpace {
				continue
			} else {
				panic(err)
			}
		}
		return
	}

	id := sm.lastid
	sm.lastid++

	sst := NewSST(id)

	sst.InsertRecord(re)

	sm.locations = append(sm.locations, int64(SST_SIZE*len(sm.headers)))
	sm.headers = append(sm.headers, sst.header)

	sm.ca.PushBack(sst)
	// 这里具体的逻辑要试情况而定
	if int64(len(sm.headers)) > 2*CACHE_NODE {
		sm.flushAll()
	}
}

func (sm *SstManager) SearchRange(low, high string) []Record {

}
func (sm *SstManager) init() {
	var err error
	if common.IsExist(sm.root + "/.sst") {
		sm.readmeta()
		sm.readChunkAndCache(16)
	} else {
		sm.f, err = os.Create(sm.root + "/.sst")
		sm.persitemeta()
	}
	if err != nil {
		panic(err)
	}
}

func (sm *SstManager) getPage(s string) *Record {

	if sm.ca.Len() > int(CACHE_NODE) {
		sm.flushPage()
	}

	sl := make([]*SortSuffixTable, 0)
	for idx, v := range sm.headers {
		if v.Min < s && s < v.Max && v.bloom.TestString(s) {

			b := make([]byte, SST_SIZE)
			sm.f.ReadAt(b, sm.locations[idx])

			sst := NewSSTDump(b)
			if sst == nil {
				panic("sst dump error")
			}
			sl = append(sl, sst)
		}
	}
	var re *Record

	defer func() {
		for _, v := range sl {
			sm.ca.PushBack(v)
		}
	}()
	for _, v := range sl {
		if re = v.SearchRecord(s); re != nil {
			return re
		}
	}
	return nil
}

func (sm *SstManager) flushPage() {

	for sm.ca.Len() > int(CACHE_NODE) {
		iter := sm.ca.Front()
		xv := iter.Value.(*SortSuffixTable)
		for idx, v := range sm.headers {
			if v.PageID == xv.header.PageID {
				b, err := xv.Dump()
				if err != nil {
					panic(err)
				}
				sm.f.WriteAt(b, sm.locations[idx])
				break
			}
		}
		sm.ca.Remove(iter)
	}
}

func (sm *SstManager) flushAll() {
	type pack struct {
		loc int64
		b   []byte
	}
	var (
		meta = make([]pack, sm.ca.Len())
	)

	for iter := sm.ca.Front(); iter != nil; iter = iter.Next() {
		xv := iter.Value.(*SortSuffixTable)
		b, err := xv.Dump()
		if err != nil {
			panic(err)
		}

		idx := bsearch(sm.headers, xv.header.PageID)
		if idx == -1 {
			continue
		}
		meta = append(meta, pack{
			loc: sm.locations[idx],
			b:   b,
		})
	}

	sort.Slice(meta, func(i, j int) bool {
		return meta[i].loc < meta[j].loc
	})

	var (
		left  = 0
		right = 0
		copys = make([]pack, 0)
	)

	for left != right && right < len(meta) {
		right++
		diff := 1
		for meta[left].loc+int64(diff*SST_SIZE) == meta[right].loc {
			right++
			diff++
		}
		t := meta[left]

		for left <= right {
			left++
			t.b = append(t.b, meta[left].b...)
		}
		copys = append(copys, t)
	}

	for _, pck := range copys {

		sm.f.WriteAt(pck.b, pck.loc)
	}
}
func (sm *SstManager) persitemeta() {

	var err error
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, sm.lastid); err != nil {
		panic(err)
	}
	if err = binary.Write(b, binary.BigEndian, sm.nums); err != nil {
		panic(err)
	}

	sm.f.WriteAt(b.Bytes(), 0)
}

func (sm *SstManager) persiteChunk() {

	for iter := sm.ca.Front(); iter != nil; iter = iter.Next() {
		n := iter.Value.(*SortSuffixTable)
		for idx, v := range sm.headers {
			if n.header.PageID == v.PageID {
				loc := sm.locations[idx]
				b, err := n.Dump()
				if err != nil {
					panic(err)
				}
				sm.f.WriteAt(b, loc)
				break
			}
		}
	}
}

// 读取头部
func (sm *SstManager) readmeta() {

	if err = binary.Read(sm.f, binary.BigEndian, &sm.lastid); err != nil {
		panic(err)
	}
	if err = binary.Read(sm.f, binary.BigEndian, &sm.nums); err != nil {
		panic(err)
	}
}
func (sm *SstManager) readChunkAndCache(size int) {
	var (
		err  error
		b    []byte = make([]byte, size*SST_SIZE)
		off  int64
		bys  int = int(unsafe.Sizeof(sm.lastid) + unsafe.Sizeof(sm.nums))
		nums int
	)

	rands := common.GenBatchRand(int(CACHE_NODE), sm.nums)

	hit := func(i int) bool {
		for _, v := range rands {
			if v == i {
				return true
			}
		}
		return false
	}
	for {
		bys, err = sm.f.ReadAt(b, off)

		if err != nil {
			if err == io.EOF {
				if bys != 0 {
					goto con
				}
			} else {
				panic(err)
			}
		}
	con:
		off += int64(bys)
		for i := 0; i < bys; i += SST_SIZE {
			nm := NewSSTDump(b[i : i+SST_SIZE])
			if nm == nil {
				panic("unformat sst")
			}
			sm.headers = append(sm.headers, nm.header)
			sm.locations = append(sm.locations, off+int64(i))

			if hit(nums) {
				sm.ca.PushBack(nm)
			}
			nums++
		}

		if err == io.EOF {
			break
		}
	}

}

func bsearch(p []PageHeader, id int64) int {
	var (
		left  int = 0
		mid   int = len(p) / 2
		right int = len(p) - 1
	)

	for left <= right {
		mid = left + (right-left)/2
		if id > p[mid].PageID {
			left = mid + 1
		} else if id < p[mid].PageID {
			right = mid - 1
		} else {
			return mid
		}
	}

	if mid < right {
		return mid
	}

	return -1
}
