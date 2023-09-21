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

type TrieIndexManager struct {
	sync.RWMutex
	disk   types.IndexDiskManager
	ca     types.Cache
	exit   chan struct{}
	perCh  chan struct{}
	fields map[string]*internal.Trie
	root   string
}

func NewTrieIndexManager(root string) *TrieIndexManager {
	tim := &TrieIndexManager{
		disk:   disk.NewAofIndexDiskManager(root),
		root:   root,
		exit:   make(chan struct{}),
		fields: make(map[string]*internal.Trie),
	}

	tim.init(64)
	go func() {
		tick := time.NewTicker(SAVE_DISK_INTERVAL)
		per := time.NewTicker(PERSITE_INTERVAL)
		flush := false
		for {
			select {
			case <-tick.C:
				tim.disk.SaveMeta()
			case <-per.C:
				if flush { //时钟到期，并且上一次间隔中至少有一次更改
					tim.persite()
					flush = false
				}
			case <-tim.perCh:
				flush = true
			case <-tim.exit:
				return
			}
		}
	}()
	return tim
}

func (tim *TrieIndexManager) init(cap int64) {

	tim.ca = cache.NewLruCache(cap, func(s string) interface{} {
		id, field := common.SpiltI64AndString(s)
		return tim.disk.GetIndex(id, field)
	}, nil)

	tim.load()
}
func (tim *TrieIndexManager) meta() string {
	return "tim.meta"
}
func (tim *TrieIndexManager) notifySave() {
	tim.perCh <- struct{}{}
}
func (tim *TrieIndexManager) persite() {
	path := tim.root + "/" + tim.meta()
	var f *os.File
	if common.IsExist(path) {
		f, _ = os.Open(path)
	} else {
		f, _ = os.Create(path)
	}

	maps := make(map[string][]byte)
	for k, v := range tim.fields {
		maps[k] = v.Serial()
	}
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)

	e.Encode(maps)
	f.Write(buf.Bytes())
	f.Close()
}

func (tim *TrieIndexManager) load() {
	path := tim.root + "/" + tim.meta()

	var f *os.File

	if common.IsExist(path) {
		f, _ = os.Open(path)
		defer f.Close()
		maps := make(map[string][]byte)

		d := gob.NewDecoder(f)

		d.Decode(&maps)

		for k, v := range maps {
			tree := internal.NewTrie()
			tree.Dump(v)
			tim.fields[k] = tree
		}
	}
}

func (tim *TrieIndexManager) AddIndex(token string, index types.Index) {
	var (
		field *internal.Trie
		ok    bool
	)
	field, ok = tim.fields[index.Field()]
	defer tim.notifySave()
	if !ok {
		field = internal.NewTrie()
		tim.fields[index.Field()] = field
		tim.persite()
	}
	field.Insert(token, index.UUID())

	tim.disk.AddIndex(index)
}

func (tim *TrieIndexManager) GetIndex(token, field string) types.Index {
	var (
		rix *internal.Trie
		ok  bool
	)

	rix, ok = tim.fields[field]
	if !ok {
		return nil
	}
	xid := rix.Search(token)
	if xid == nil {
		return nil
	}
	return tim.disk.GetIndex(xid.(int64), field)
}
