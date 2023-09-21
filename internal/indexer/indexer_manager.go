package indexer

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"fts/internal/common"
	"fts/internal/document"
	"fts/internal/types"
	"log"
	"os"
	"reflect"
	"time"
)

type BuildInfo struct {
	DocID    int64
	IndexIDS []int64
}

var (
	meta = "irm.meta"
)

type IndexerManager struct {
	root    string
	fields  map[string][]string    // doc-type -> fields
	batch   map[string][]BuildInfo // fields -> buildinfe
	lens    map[string]int64
	indexer *Indexer

	batchSize int
	f         func([]BuildInfo, error) error
	s         chan struct{}
}

type buildResult struct {
	idx     int
	indexes map[string]types.Index
	err     error
}

func NewIndexerManager(root string, builder types.IndexBuilder) *IndexerManager {

	im := &IndexerManager{
		root:    root,
		indexer: NewIndexer(builder),
		fields:  make(map[string][]string),
		batch:   make(map[string][]BuildInfo),
		lens:    make(map[string]int64),
	}
	im.init()
	return im
}

func (im *IndexerManager) init() {
	im.load()
}

func (im *IndexerManager) persite() {
	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)

	e.Encode(&im.batch)
	e.Encode(&im.fields)

	f, _ := os.Open(im.root + "/" + meta)
	defer f.Close()

	f.Write(buf.Bytes())
}

func (im *IndexerManager) load() {
	path := im.root + "/" + meta
	if common.IsExist(path) {
		f, _ := os.Open(path)
		defer f.Close()
		d := gob.NewDecoder(f)

		if d.Decode(&im.batch) != nil || d.Decode(&im.fields) != nil {
			panic("indexer-manager decode meta file error")
		}
	}
}
func (im *IndexerManager) SetBatchSize(b int) {
	im.batchSize = b
}
func (im *IndexerManager) OnBuild(on func([]BuildInfo, error) error) {
	im.f = on
}

func (im *IndexerManager) waitBatch(
	batches []types.Document,
	field string,
	in types.IndexManager,
	cores int,
) error {
	resCh := make(chan buildResult, 256)
	compelete := make(chan struct{}, cores)
	im.indexer.BatchBuild(cores, batches, field, resCh, compelete)
	indexes := make(map[string]types.Index)
	//dealine := time.Now().Add(3 * time.Second)
	info := make([]BuildInfo, len(batches))

	for i, v := range batches {
		info[i] = BuildInfo{
			DocID: v.UUID(),
		}
	}

	total := 0
	var err error
	for {
		select {
		case res := <-resCh:
			if res.err != nil {
				err = res.err
				goto l
			}
			for _, v := range res.indexes {
				info[res.idx].IndexIDS = append(info[res.idx].IndexIDS, v.UUID())
			}
			for k, v := range res.indexes {
				s, ok := indexes[k]
				if ok {
					MergeTwoIndex(s, v)
				} else {
					indexes[k] = v
				}
			}
		case <-compelete:
			total++
			if total == cores {
				goto l
			}
		case <-time.After(5 * time.Second):
			return errors.New("fetch build result timeout")
		}
	}
l:
	if im.f != nil {
		err = im.f(info, err)
	}
	if err != nil {
		return err
	}
	//im.batch[field] = append(im.batch[field], info...)
	im.AddBuildInfo(field, info)
	for k, v := range indexes {
		in.AddIndex(k, v)
	}
	return nil
	// success += im.batchSize

	// batches = []types.Document{}
}

func (im *IndexerManager) freshFieldBuild(
	typ types.Document,
	field string,
	doc *document.DocumentManager,
	in types.IndexManager,
) error {
	ch := doc.ChanDocsID(typ)

	batches := []types.Document{}
	for {
		var i64 int64
		var ok bool
		select {
		case i64, ok = <-ch:
			if !ok {
				return nil
			}
		case <-time.After(100 * time.Millisecond): //单次取值不得超过100毫秒
			return fmt.Errorf("read doc-id channel timeout")
		}
		dc := doc.GetDocument(i64)
		batches = append(batches, dc)
		if len(batches) == im.batchSize {
			can := im.waitBatch(batches, field, in, 8)
			if can != nil {
				return can
			}
			im.lens[field] += int64(im.batchSize)
			batches = []types.Document{}
		}

		im.persite()
	}
}

func (im *IndexerManager) checkBuild(
	typ types.Document,
	field string,
	doc *document.DocumentManager,
	in types.IndexManager,
) error {

	ch := doc.ChanDocsID(typ)
	build := []types.Document{}
	for {
		var i64 int64
		var ok bool
		select {
		case i64, ok = <-ch:
			if !ok {
				return nil
			}
		}

		inf := im.LookupBuildInfo(field, i64)
		if inf == nil {
			build = append(build, doc.GetDocument(i64))
		}

		if len(build) == im.batchSize {
			err := im.waitBatch(build, field, in, 8)
			if err != nil {
				return nil
			}
		}

		im.lens[field] += int64(im.batchSize)
		im.persite()
	}
}

func (im *IndexerManager) BuildIndex(
	typ types.Document,
	field string,
	doc *document.DocumentManager,
	in types.IndexManager,
) error {

	name := common.ExtractMetaTypeName(reflect.TypeOf(typ))
	dc, ok := im.fields[name]
	if !ok {
		im.fields[name] = []string{field}
		im.lens[field] = 0

		return im.freshFieldBuild(typ, field, doc, in)
	} else {
		total := doc.Docs(typ)

		for _, v := range dc {
			if v == field && total > im.lens[v] {
				return im.checkBuild(typ, field, doc, in)
			}
		}

		if im.lens[field] == 0 {
			im.freshFieldBuild(typ, field, doc, in)
		} else {
			log.Println("already build")
		}
	}

	return nil
}

func MergeTwoIndex(i1 types.Index, i2 types.Index) {
	i1.Merge(i2)
}

func (im *IndexerManager) AddBuildInfo(field string, bi []BuildInfo) {
	arr, ok := im.batch[field]
	if !ok {
		arr = make([]BuildInfo, 0)
		im.batch[field] = arr
	}
	for _, v := range bi {
		idx := len(arr) - 1
		for ; idx >= 0; idx-- {
			if arr[idx].DocID < v.DocID {
				break
			}
		}
		arr = append(arr[idx:], append([]BuildInfo{
			{
				DocID:    v.DocID,
				IndexIDS: v.IndexIDS,
			},
		}, arr[idx+1:]...)...)
	}
}

func (im *IndexerManager) LookupBuildInfo(field string, docID int64) *BuildInfo {
	arr := im.batch[field]
	low, high := 0, len(arr)-1
	for low <= high {
		mid := (low + high) / 2
		if arr[mid].DocID == docID {
			return &arr[mid]
		} else if arr[mid].DocID < docID {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return nil
}
