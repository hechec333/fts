package document

import (
	"fmt"
	"fts/internal/cache"
	"fts/internal/types"
	"strconv"
)

var maxloads = 4096

type DocumentWrapper struct {
	SerailType int
}

// Document Manager
type DocumentManager struct {
	cache types.Cache
	disk  types.DocDiskManager
}

func NewDocumentManager(cap int64, disk types.DocDiskManager) *DocumentManager {
	dm := &DocumentManager{
		disk: disk,
	}

	dm.init(cap)
	return dm
}

func (dm *DocumentManager) init(cap int64) {
	dm.cache = cache.NewLruCache(cap, func(key string) interface{} {
		return dm.fetchDisk(key)
	}, func(key string, dt interface{}) {
		dm.Sync(key, dt)
	})
}

func (dm *DocumentManager) LoadDocument(loader types.DocumentLoader) {
	ch := make(chan types.Document, maxloads)
	Errch := make(chan error)
	//go LoadAbstractDocumentGzip(path, ty, ch, Errch)
	go loader.Load(ch, Errch)
	count := 0
	for {
		select {
		case err := <-Errch:
			if err != nil {
				loader.ErrExit(err)
				panic(err)
			}
		case doc := <-ch:
			if doc == nil {
				goto e
			}
			count++
			dm.disk.AddDoc(doc)
		}
	}
e:
	dm.FlushAllBuildCache()
	dm.disk.SaveMeta()
	fmt.Printf("success loaded %v documents", count)
}
func (dm *DocumentManager) GetDocument(ID int64) types.Document {
	var doc interface{}
	var ok bool
	xid := strconv.FormatInt(ID, 10)
	doc, ok = dm.cache.Get(xid)
	if !ok {
		return nil
	}
	return doc.(types.Document)
}
func (dm *DocumentManager) fetchDisk(ID string) interface{} {
	doc, ok := dm.Miss(ID)
	if !ok {
		return nil
	}
	return doc
}

func (dm *DocumentManager) Docs(doc types.Document) int64 {
	return dm.disk.Docs(doc)
}

func (dm *DocumentManager) ChanDocsID(doc types.Document) chan int64 {

	return dm.disk.EnumDocsID(doc, 1024)
}

// implent DiskManager
func (dm *DocumentManager) Miss(key string) (interface{}, bool) {
	i64, _ := strconv.ParseInt(key, 10, 64)
	in := dm.disk.GetDoc(i64)

	return in, in != nil
}

func (dm *DocumentManager) Sync(key string, data interface{}) {
	// 不会改动interface
	return
}

func (dm *DocumentManager) DumpAllDocsID() []int64 {
	size := 4096
	sl := make([]int64, 0)
	typs := dm.disk.EnumDocTypes()

	for _, v := range typs {
		ch := dm.disk.EnumDocsID(v, size)
		for {
			id, ok := <-ch
			if !ok {
				break
			}
			sl = append(sl, id)
		}
	}

	return sl
}

func (dm *DocumentManager) FlushAllBuildCache() {
	dm.disk.Flush()
}
