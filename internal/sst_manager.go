package internal

import (
	"container/list"
	"fts/internal/common"
	"io"
	"os"
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
	var err error
	sstm.f, err = os.Open(root)

	if err != nil {
		panic(err)
	}

	sstm.init()

	return sstm
}

func (sm *SstManager) init() {
	sm.readmeta()
	sm.readChunkAndCache(16)

}

func (sm *SstManager) getPage(s string) *SortSuffixTable {

}

func (sm *SstManager) flushPage(s *SortSuffixTable) {

}

// 读取头部
func (sm *SstManager) readmeta() {

}
func (sm *SstManager) readChunkAndCache(size int) {
	var (
		err  error
		b    []byte = make([]byte, size*SST_SIZE)
		off  int64
		bys  int
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
		}

		if err == io.EOF {
			break
		}
	}

}
