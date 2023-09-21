package disk

import (
	"encoding/gob"
	"fts/internal/cache"
	"fts/internal/common"
	"fts/internal/types"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

var (
	maxElem = 65535 //cache token
)

type chunkInfo struct {
	sync.RWMutex
	offset map[int64]int64
	lens   map[int64]int64
	links  map[int64][]int64
}

func (ici *chunkInfo) Size() (res int64) {
	for _, v := range ici.lens {
		res += int64(v)
	}
	return
}

func (ici *chunkInfo) AofPadding(path string, off int64, id int64, sl []byte) {
	nid := common.GetInt64()
	ff, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0777)
	defer ff.Close()
	if _, ok := ici.links[id]; !ok {
		ici.links[id] = make([]int64, 0)
	}
	ici.links[id] = append(ici.links[id], nid)
	ici.lens[id] = nid
	ici.offset[id] = off
	ff.Write(sl)
}
func (ici *chunkInfo) add(id int64, offset int64, lens int64) {
	ici.links[id] = []int64{id}
	ici.lens[id] = lens
	ici.offset[id] = offset
}
func (ici *chunkInfo) find(id int64) (int64, int64, error) {
	aofs := ici.links[id]
	if len(aofs) > 1 {
		most := aofs[len(aofs)-1]
		return ici.offset[most], ici.offset[most], nil
	}

	return ici.offset[id], ici.lens[id], nil
}

// 最新数据所占的比列
func (ici *chunkInfo) goal() float64 {
	size := ici.Size()
	var osize int64
	for _, v := range ici.links {
		cid := v[len(v)-1]
		lns := ici.lens[cid]
		osize += int64(lns)
	}
	return float64(osize) / float64(size)
}
func (ici *chunkInfo) shouldCompact() bool {
	return ici.goal() < 0.4
}

// 提取有用信息，压缩至w中
func (ici *chunkInfo) compactTo(path string, newPath string) []int64 {
	w, _ := os.Create(newPath)
	defer w.Close()
	b := make([]byte, ici.Size())

	f, _ := os.Open(path)
	defer f.Close()
	bys := []byte{}
	f.Read(b)
	ids := []int64{}
	for k, v := range ici.links {
		ids = append(ids, k)
		if len(v) == 0 {
			offset := ici.offset[k]
			lens := ici.lens[k]
			bys = append(bys, b[offset:offset+lens]...)
		} else {
			most := v[len(v)-1]
			offset := ici.offset[most]
			lens := ici.lens[most]
			bys = append(bys, b[offset:offset+lens]...)
			ici.links[k] = []int64{}
		}
	}

	w.Write(bys)

	return ids
}

type AofIndexDiskManager struct {
	sync.RWMutex
	root       string
	segments   map[string][]int64
	chunks     map[string]*chunkInfo
	locks      map[string]*sync.Mutex
	fields     map[string]reflect.Type
	codec      types.DiskCodec
	blockcache types.Cache
}

func NewAofIndexDiskManager(root string) *AofIndexDiskManager {
	ridm := &AofIndexDiskManager{
		root:     root,
		segments: make(map[string][]int64),
		chunks:   make(map[string]*chunkInfo),
		fields:   make(map[string]reflect.Type),
	}

	ridm.init()

	return ridm
}
func (ridm *AofIndexDiskManager) meta() string {
	return "aidm_idx.meta"
}
func (ridm *AofIndexDiskManager) init() {
	ridm.blockcache = cache.NewLruCache(int64(maxElem), func(s string) interface{} {
		id, _ := strconv.ParseInt(s, 10, 64)
		return ridm.getFromDisk(id)
	}, func(s string, i interface{}) {
		id, _ := strconv.ParseInt(s, 10, 64)
		path, _ := ridm.fetchID(id)

		go ridm.fflushBytes(id, path, i.([]byte))
	})
	ridm.loadMeta()

	go ridm.compact()
}

func (ridm *AofIndexDiskManager) compact() {
	tick := time.NewTicker(5 * time.Second)

	for {
		<-tick.C
		for k, v := range ridm.chunks {
			if v.shouldCompact() {
				npath := ridm.root + "/" + strconv.Itoa(len(ridm.segments)) + ".idx"
				v.compactTo(k, npath)
				ridm.chunks[npath] = v
				delete(ridm.chunks, k)
				os.Rename(k, k+".del")
			}
		}
	}
}

func (ridm *AofIndexDiskManager) getFromDisk(id int64) []byte {
	path, ok := ridm.fetchID(id)
	if !ok {
		return nil
	}
	return ridm.fetchBytes(id, path)
}

func (ridm *AofIndexDiskManager) loadMeta() {
	ridm.Lock()
	defer ridm.Unlock()
	path := ridm.root + "/" + ridm.meta()
	if common.IsExist(path) {
		f, _ := os.Open(path)
		defer f.Close()
		d := gob.NewDecoder(f)
		d.Decode(&ridm.segments)
		d.Decode(&ridm.chunks)
		d.Decode(&ridm.fields)
	}
}

func (ridm *AofIndexDiskManager) persite() {
	ridm.Lock()
	defer ridm.Unlock()

	path := ridm.root + "/" + ridm.meta()
	var f *os.File
	defer f.Close()
	if common.IsExist(path) {
		f, _ = os.Open(path)
	} else {
		f, _ = os.Create(path)
	}

	e := gob.NewEncoder(f)
	e.Encode(&ridm.segments)
	e.Encode(&ridm.chunks)
	e.Encode(&ridm.fields)
}

func (ridm *AofIndexDiskManager) fetchBytes(id int64, file string) []byte {
	ridm.RLock()
	defer ridm.RUnlock()

	f, _ := os.Open(file)
	ck := ridm.chunks[file]

	b := make([]byte, ck.lens[id])

	f.ReadAt(b, ck.offset[id])
	return b
}

// 如果
func (ridm *AofIndexDiskManager) fflushBytes(id int64, file string, b []byte) {
	ridm.RLock()
	defer ridm.RUnlock()
	ck := ridm.chunks[file]
	offset, lens, _ := ck.find(id)
	if _, ok := ridm.locks[file]; !ok {
		ridm.locks[file] = &sync.Mutex{}
	}

	mu := ridm.locks[file]
	mu.Lock()
	defer mu.Unlock()
	f, _ := os.Open(file)
	defer f.Close()
	//这种覆盖式写入是一种极为低效的方式
	//改为增量复制，AOF
	size := common.GetFileSize(f)
	oldlens := common.Max(size-offset-lens, 0) // ==0?
	by := make([]byte, int(oldlens)+len(b))
	if oldlens == 0 {
		copy(by, b)
		f.WriteAt(by, offset)
		lens = int64(len(b))
	} else {
		f.ReadAt(by, offset)
		s1 := common.GetSha256(by[:len(b)])
		if common.GetSha256(b) == s1 {
			//内容一致
			return
		}
		sl := by[len(b):]
		ck.AofPadding(file, id, size, sl)
	}
}

// 保证有序
func (idm *AofIndexDiskManager) addFileID(path string, id int64) {
	// lock topdown

	_, ok := idm.segments[path]
	if !ok {
		idm.segments[path] = []int64{}
	}

	arr := idm.segments[path]
	i := len(arr) - 1
	for {
		if i >= 0 && arr[i] > id {
			i--
		} else {
			break
		}
	}

	if i == len(arr)-1 {
		arr = append(arr, id)
	} else if i == -1 {
		arr = append([]int64{id}, arr...)
	} else {
		arr = append(arr[i:], append([]int64{id}, arr[i+1:]...)...) // 在i处插入
	}
	idm.segments[path] = arr
}
func (idm *AofIndexDiskManager) fetchID(id int64) (string, bool) {
	var path string
	if len(idm.segments) > 256 {
		cores := 4
		if len(idm.segments) > 4096 {
			cores = 8
		}

		lens := len(idm.segments)
		ck := lens / cores
		success := make(chan string)
		complte := make(chan struct{})
		slice := make([]string, lens)
		j := 0
		for k := range idm.segments {
			slice[j] = k
			j++
		}

		for i := 0; i < cores; i++ {
			go func(seq int) {
				up := common.Min((seq+1)*ck, lens)
				for z := seq * ck; z < up; z++ {
					if common.BinarySearch(idm.segments[slice[z]], id) {
						success <- slice[z]
						goto l
					}
				}
			l:
				complte <- struct{}{}
			}(i)
		}
		count := 0

		for {
			select {
			case path = <-success:
				return path, true
			case <-complte:
				count++
				if count == cores && path == "" {
					return path, false
				}
			}
		}
	}

	for p, v := range idm.segments {
		if common.BinarySearch(v, id) {
			return p, true
		}
	}

	return path, false
}

func (ridm *AofIndexDiskManager) getIndex(key string, fields string) types.Index {
	//id, _ := strconv.ParseInt(key, 10, 64)
	in, ok := ridm.blockcache.Get(key)
	if !ok {
		return nil
	}
	tyzm, ok := ridm.fields[fields]
	if !ok {
		return nil
	}
	index := reflect.New(tyzm).Interface().(types.Index)
	index.Dump(in.([]byte))
	return index
}

func (ridm *AofIndexDiskManager) addIndex(index types.Index) {
	ridm.Lock()
	defer ridm.Unlock()
	if _, ok := ridm.fields[index.Field()]; !ok {
		ridm.fields[index.Field()] = reflect.TypeOf(index)
	}
	id := index.UUID()
	bytes := index.Serial()
	if xpath, ok := ridm.fetchID(id); ok {
		ridm.mergeIndex(xpath, id, index)
		ridm.addFileID(xpath, id)
		return
	}
	var ck *chunkInfo
	var path string

	for p, v := range ridm.chunks {
		if v.Size()+int64(len(bytes)) < int64(max) {
			path = p
			ck = v
			break
		}
	}
	if path != "" {
		f, _ := os.Open(path)
		defer f.Close()
		off := common.GetFileSize(f)
		ck.offset[id] = off
		ck.lens[id] = int64(len(bytes))
		ridm.addFileID(path, id)
		ridm.blockcache.Put(strconv.FormatInt(id, 10), bytes)
	} else {
		path = ridm.root + "/" + strconv.Itoa(len(ridm.segments)) + ".idx"
		f, _ := os.Create(path)
		defer f.Close()
		ridm.chunks[path] = &chunkInfo{
			offset: map[int64]int64{
				id: 0,
			},
			lens: map[int64]int64{
				id: int64(len(bytes)),
			},
			links: map[int64][]int64{
				id: make([]int64, 0),
			},
		}
		ridm.addFileID(path, id)
		ridm.blockcache.Put(strconv.FormatInt(id, 10), bytes)
	}
}

func (ridm *AofIndexDiskManager) mergeIndex(path string, id int64, index types.Index) {
	//go ridm.fflushBytes(id, path, index.Bytes())
	xid := strconv.FormatInt(id, 10)

	in := reflect.New(reflect.TypeOf(index)).Interface().(types.Index)
	b, _ := ridm.blockcache.Get(xid)
	in.Dump(b.([]byte))
	if !index.Merge(in) {
		return
	}
	ridm.blockcache.Put(xid, index.Serial())
}
func (ridm *AofIndexDiskManager) checkSha256(sha256 string) bool {
	return true
}
func (ridm *AofIndexDiskManager) SaveMeta() {
	ridm.persite()
}

func (ridm *AofIndexDiskManager) EnumFields() (attr []string) {
	for field := range ridm.fields {
		attr = append(attr, field)
	}
	return
}
func (ridm *AofIndexDiskManager) GetIndex(id int64, f string) types.Index {
	xid := strconv.FormatInt(id, 10)
	return ridm.getIndex(xid, f)
}
func (ridm *AofIndexDiskManager) AddIndex(i types.Index) {
	ridm.addIndex(i)
}
func (ridm *AofIndexDiskManager) Close() {

	ridm.blockcache.Clear()
	// 将全部缓冲区内容刷往磁盘
	for path := range ridm.chunks {
		var f *os.File
		f, _ = os.Open(path)
		f.Sync()
		f.Close()
	}

}
