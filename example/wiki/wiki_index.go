package main

import (
	"bytes"
	"encoding/gob"
	"fts/internal/common"
	"fts/internal/types"
)

type WikiAbstractIndex struct {
	Docs  map[int64]int16
	Token string `xml:"tokens"`
}

// type Index interface {
// 	Serializer
// 	Field() string
// 	UUID() int64
// 	Merge(interface{}) bool
// 	QueryDoc(int64) int16 //查询docid是否在这个索引中，返回出现次数
// 	QueryAllDoc() IndexQueryResult
// }

func (wai *WikiAbstractIndex) meta() string {
	return "wiki"
}

func (wai *WikiAbstractIndex) field() string {
	return "Abstract"
}

func (wai *WikiAbstractIndex) Serial() []byte {

	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)

	e.Encode(wai)

	return buf.Bytes()
}

func (wai *WikiAbstractIndex) Dump(b []byte) {

	d := gob.NewDecoder(bytes.NewReader(b))

	d.Decode(wai)
}

func (wai *WikiAbstractIndex) Field() string {
	return wai.field()
}

func (wai *WikiAbstractIndex) UUID() int64 {
	sid := common.MergeString(wai.meta(), wai.field(), wai.Token)
	return common.StringHashToInt64(sid)
}

func (wai *WikiAbstractIndex) Merge(i interface{}) bool {

	in, ok := i.(WikiAbstractIndex)
	if !ok {
		return false
	}
	if in.Token != wai.Token {
		return false
	}
	for id, v := range in.Docs {
		xid, ok := wai.Docs[id]
		if ok {
			wai.Docs[id] += xid
		} else {
			wai.Docs[id] = v
		}
	}
	return true

}

func (wai *WikiAbstractIndex) QueryDoc(id int64) int16 {
	return wai.Docs[id]
}

func (wai *WikiAbstractIndex) QueryAllDoc() types.IndexQueryResult {
	res := types.IndexQueryResult{
		Info: wai.Docs,
	}
	ids := []int64{}
	for k := range wai.Docs {
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
