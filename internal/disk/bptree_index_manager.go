package disk

import (
	"bytes"
	"encoding/gob"
	"fts/internal"
	"fts/internal/cache"
	"fts/internal/common"
	"fts/internal/types"
	"os"
	"reflect"
)

type BPIndexDiskManager struct {
	fileds   map[string]*internal.BPlusTree
	reflects map[string]reflect.Type
	zc       types.ZCache
	root     string
}

func NewBPIndexDiskManager(root string) *BPIndexDiskManager {
	bpidm := &BPIndexDiskManager{
		root:   root,
		fileds: make(map[string]*internal.BPlusTree),
	}
	bpidm.init()

	return bpidm
}

func (bpi *BPIndexDiskManager) init() {
	zc := cache.NewZLruCache(16)
	zc.BindMissed(func(s string) interface{} {
		id, field := common.SpiltI64AndString(s)
		return bpi.getIndex(id, field)
	})

	zc.BindEvited(func(s string, in interface{}) {
		id, field := common.SpiltI64AndString(s)
		bp := bpi.fileds[field]

		bp.InsertOrUpdateWhat(uint64(id), func(is bool, s string) string {
			if is {
				return string(in.(types.Index).Serial())
			} else {
				index := in.(types.Index)
				// new a index
				v := reflect.New(reflect.TypeOf(index)).Interface().(types.Index)
				v.Dump([]byte(s))
				index.Merge(v)
				return string(index.Serial())
			}
		})
	})

	bpi.load()
}
func (bpi *BPIndexDiskManager) meta() string {
	return "bpidm_idx.meta"
}

func (bpi *BPIndexDiskManager) load() {
	maps := make(map[string]string)
	path := bpi.root + "/" + bpi.meta()
	if common.IsExist(path) {
		f, _ := os.Open(path)

		defer f.Close()

		d := gob.NewDecoder(f)

		d.Decode(&maps)
		d.Decode(&bpi.reflects)
		for field, path := range maps {
			b, err := internal.NewBPlusTree(path)
			if err != nil {
				panic(err)
			}

			bpi.fileds[field] = b
		}
	}
}
func (bpi *BPIndexDiskManager) persite() {
	buf := new(bytes.Buffer)
	maps := make(map[string]string)
	e := gob.NewEncoder(buf)
	for k, v := range bpi.fileds {
		maps[k] = v.Path()
	}
	e.Encode(maps)
	e.Encode(bpi.reflects)
	var f *os.File
	if common.IsExist(bpi.root) {
		f, _ = os.Open(bpi.root + "/" + bpi.meta())
	} else {
		f, _ = os.Create(bpi.root + "/" + bpi.meta())
	}
	defer f.Close()
	f.Write(buf.Bytes())
}
func (bpi *BPIndexDiskManager) getIndex(id int64, field string) types.Index {
	bp := bpi.fileds[field]
	typ := bpi.reflects[field]
	if bp == nil || typ == nil {
		return nil
	}
	index := reflect.New(typ).Interface().(types.Index)
	b, ok := bp.Find(uint64(id))
	if ok != nil {
		return nil
	}

	index.Dump([]byte(b))
	return index
}
func (bpi *BPIndexDiskManager) addIndex(index types.Index) {
	_, ok := bpi.fileds[index.Field()]
	if !ok {
		bpi.fileds[index.Field()], _ = internal.NewBPlusTree(index.Field() + "_bp.idx")
		bpi.reflects[index.Field()] = reflect.TypeOf(index)
		bpi.persite()
	}
	key := common.MergeI64AndString(index.UUID(), index.Field())
	bpi.zc.Put(key, index)
}
func (bpi *BPIndexDiskManager) AddIndex(index types.Index) {
	bpi.addIndex(index)
}

func (bpi *BPIndexDiskManager) GetIndex(id int64, field string) types.Index {
	return bpi.getIndex(id, field)
}

func (bpi *BPIndexDiskManager) SaveMeta() {
	bpi.persite()
}

func (bpi *BPIndexDiskManager) EnumFields() (s []string) {
	for k := range bpi.reflects {
		s = append(s, k)
	}
	return
}

func (bpi *BPIndexDiskManager) Close() {
	bpi.zc.Clear()
}
