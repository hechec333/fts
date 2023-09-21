package main

import (
	"bytes"
	"encoding/gob"
	"fts/internal/common"
	"fts/internal/types"
)

// type Index interface {
// 	Serializer
// 	Field() string
// 	UUID() int64
// 	Merge(interface{}) bool
// 	QueryDoc(int64) int16 //查询docid是否在这个索引中，返回出现次数
// 	QueryAllDoc() IndexQueryResult
// }

// implement Index
type SinaTitleIndex struct {
	Maps  map[int64]int16 `xml:"Docs"`
	Token string          `xml:"Token"`
}

func (sti *SinaTitleIndex) meta() string {
	return "wiki"
}

func (sti *SinaTitleIndex) field() string {
	return "Title"
}

func (sti *SinaTitleIndex) Dump(b []byte) {
	d := gob.NewDecoder(bytes.NewReader(b))

	d.Decode(sti)
}

func (sti *SinaTitleIndex) Serial() []byte {
	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)

	e.Encode(sti)

	return buf.Bytes()
}

func (sti *SinaTitleIndex) Field() string {
	return sti.field()
}

func (sti *SinaTitleIndex) UUID() int64 {
	sid := common.MergeString(sti.meta(), sti.field(), sti.Token)
	return common.StringHashToInt64(sid)
}

func (sti *SinaTitleIndex) Merge(i interface{}) bool {
	in, ok := i.(SinaTitleIndex)
	if !ok {
		return false
	}
	if in.Token != sti.Token {
		return false
	}
	for id, v := range in.Maps {
		xid, ok := sti.Maps[id]
		if ok {
			sti.Maps[id] += xid
		} else {
			sti.Maps[id] = v
		}
	}
	return true
}

func (sti *SinaTitleIndex) QueryDoc(id int64) int16 {
	return sti.Maps[id]
}

func (sti *SinaTitleIndex) QueryAllDoc() types.IndexQueryResult {
	res := types.IndexQueryResult{
		Info: sti.Maps,
	}
	ids := []int64{}
	for k := range sti.Maps {
		idx := 0
		for ; idx < len(ids); idx++ {
			if ids[idx] > k {
				break
			}
		}

		if idx == len(ids) {
			ids = append(ids, k)
		} else {
			ids = append(ids[:idx], append([]int64{k}, ids[idx+1:]...)...)
		}
	}

	res.Ids = ids
	return res
}
