package index

import (
	"bytes"
	"encoding/gob"
	"fts/internal"
	"fts/internal/cache"
	"fts/internal/common"
	"fts/internal/disk"
	"fts/internal/types"
	"os"
	"sync"
	"time"
)

type RadixIndexManager struct {
	sync.RWMutex
	cache types.Cache
	disk  types.IndexDiskManager
	exit  chan struct{}
	perCh chan struct{}
	radix map[string]*internal.RadixTree
	root  string
}

func NewRadixIndexDiskManager(root string) *RadixIndexManager {
	rim := &RadixIndexManager{
		radix: make(map[string]*internal.RadixTree),
		disk:  disk.NewAofIndexDiskManager("."),
		exit:  make(chan struct{}),
		root:  root,
		perCh: make(chan struct{}),
	}
	rim.cache = cache.NewLruCache(1024, func(s string) interface{} {
		i, fe := common.SpiltI64AndString(s)
		return rim.fetchIndex(i, fe)
	}, nil)
	go func() {
		tick := time.NewTicker(SAVE_DISK_INTERVAL)
		per := time.NewTicker(PERSITE_INTERVAL)
		flush := false
		for {
			select {
			case <-tick.C:
				rim.disk.SaveMeta()
			case <-per.C:
				if flush { //时钟到期，并且上一次间隔中至少有一次更改
					rim.persite()
					flush = false
				}
			case <-rim.perCh:
				flush = true
			case <-rim.exit:
				return
			}
		}
	}()

	return rim
}
func (rim *RadixIndexManager) meta() string {
	return "rim.meta"
}
func (rim *RadixIndexManager) notifySave() {
	rim.perCh <- struct{}{}
}
func (rim *RadixIndexManager) persite() {
	path := rim.root + "/" + rim.meta()
	var f *os.File
	if common.IsExist(path) {
		f, _ = os.Open(path)
	} else {
		f, _ = os.Create(path)
	}

	maps := make(map[string][]byte)
	for k, v := range rim.radix {
		maps[k] = v.Serial()
	}
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)

	e.Encode(maps)
	f.Write(buf.Bytes())
	f.Close()
}

func (rim *RadixIndexManager) load() {
	path := rim.root + "/" + rim.meta()

	var f *os.File

	if common.IsExist(path) {
		f, _ = os.Open(path)
		defer f.Close()
		maps := make(map[string][]byte)

		d := gob.NewDecoder(f)

		d.Decode(&maps)

		for k, v := range maps {
			tree := internal.NewRadixTree()
			tree.Dump(v)
			rim.radix[k] = tree
		}
	}
}

func (rim *RadixIndexManager) Close() {
	rim.exit <- struct{}{}
}
func (rim *RadixIndexManager) fetchIndex(i int64, fe string) types.Index {
	return rim.disk.GetIndex(i, fe)
}
func (rim *RadixIndexManager) AddIndex(token string, index types.Index) {
	rim.Lock()
	defer rim.Unlock()
	defer rim.notifySave()
	xid := index.UUID()
	field := index.Field()
	//key := common.MergeDoubleString(token, field)
	var rix *internal.RadixTree
	var ok bool
	if rix, ok = rim.radix[field]; !ok {
		rim.radix[field] = internal.NewRadixTree()
		rix = rim.radix[field]
	}
	rix.Insert(token, xid)
	rim.disk.AddIndex(index)
}

func (rim *RadixIndexManager) GetIndex(token string, fields string) types.Index {
	rim.RLock()
	defer rim.RUnlock()
	//key := common.TokenSetType(token, ty)
	rix := rim.radix[fields]
	id, ok := rix.Search(token)
	if !ok {
		return nil
	}
	key := common.MergeI64AndString(id.(int64), fields)
	i, _ := rim.cache.Get(key)
	return i.(types.Index)
}
