package index

import (
	"bytes"
	"encoding/gob"
	"fts/internal"
	"fts/internal/common"
	"fts/internal/disk"
	"fts/internal/types"
	"os"
	"time"
)

type BpIndexManager struct {
	disk  types.IndexDiskManager
	radix map[string]*internal.RadixTree
	exit  chan struct{}
	perCh chan struct{}
	root  string
}

func NewBPIndexManager(root string) *BpIndexManager {
	bpm := &BpIndexManager{
		root:  root,
		disk:  disk.NewBPIndexDiskManager(root),
		exit:  make(chan struct{}),
		radix: make(map[string]*internal.RadixTree),
		perCh: make(chan struct{}),
	}
	bpm.load()
	go func() {
		tick := time.NewTicker(SAVE_DISK_INTERVAL)
		per := time.NewTicker(PERSITE_INTERVAL)
		for {
			flush := false
			for {
				select {
				case <-tick.C:
					bpm.disk.SaveMeta()
				case <-per.C:
					if flush { //时钟到期，并且上一次间隔中至少有一次更改
						bpm.persite()
						flush = false
					}
				case <-bpm.perCh:
					flush = true
				case <-bpm.exit:
					return
				}
			}
		}
	}()
	return bpm
}
func (bpm *BpIndexManager) meta() string {
	return "bpm.meta"
}
func (bpm *BpIndexManager) notifySave() {
	bpm.perCh <- struct{}{}
}
func (bpm *BpIndexManager) persite() {
	b := new(bytes.Buffer)

	e := gob.NewEncoder(b)

	maps := make(map[string][]byte)

	for k, v := range bpm.radix {
		maps[k] = v.Serial()
	}

	e.Encode(maps)

	path := bpm.root + "/" + bpm.meta()
	var f *os.File
	if common.IsExist(path) {
		f, _ = os.Open(path)
	} else {
		f, _ = os.Create(path)
	}
	defer f.Close()
	f.Write(b.Bytes())
}

func (bpm *BpIndexManager) load() {
	path := bpm.root + "/" + bpm.meta()

	if common.IsExist(path) {
		maps := make(map[string][]byte)

		f, _ := os.Open(path)
		defer f.Close()

		d := gob.NewDecoder(f)

		d.Decode(&maps)

		for k, b := range maps {
			rt := internal.NewRadixTree()
			rt.Dump(b)
			bpm.radix[k] = rt
		}
	}
}

func (bpm *BpIndexManager) Close() {
	bpm.exit <- struct{}{}
}
func (bpm *BpIndexManager) AddIndex(token string, index types.Index) {
	field := index.Field()
	defer bpm.notifySave()
	var rix *internal.RadixTree
	var ok bool
	rix, ok = bpm.radix[field]
	if !ok {
		bpm.radix[field] = internal.NewRadixTree()
		rix = bpm.radix[field]
	}
	rix.Insert(token, index.UUID())
	bpm.disk.AddIndex(index)
}

func (bpm *BpIndexManager) GetIndex(token string, field string) types.Index {
	var (
		rix *internal.RadixTree
		ok  bool
	)

	rix, ok = bpm.radix[field]
	if !ok {
		return nil
	}
	xid, ok := rix.Search(token)
	if !ok {
		return nil
	}
	return bpm.disk.GetIndex(xid.(int64), field)
}
