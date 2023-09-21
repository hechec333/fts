package main

import (
	"compress/gzip"
	"encoding/xml"
	"fts/internal/common"
	"fts/internal/types"
	"io"
	"os"
)

type WikiDocLoader struct {
	st     chan struct{}
	target string
}

func NewWikiDocLoader(t string) *WikiDocLoader {
	return &WikiDocLoader{
		target: t,
		st:     make(chan struct{}),
	}
}

func (wdl *WikiDocLoader) Load(ch chan types.Document, che chan error) {
	var (
		f   *os.File
		err error
	)

	f, err = os.Open(wdl.target)
	if err != nil {
		che <- err
		return
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)

	if err != nil {
		che <- err
		return
	}

	defer gz.Close()
	doc := make([]Document, 32)
	d := xml.NewDecoder(gz)
	for {
		select {
		case <-wdl.st:
			return
		default:
		}
		idx := 0
		if err := d.Decode(&doc[idx]); err != nil {
			if err == io.EOF {
				break
			} else {
				che <- err
			}
		}
		idx++
		if idx == 32 {
			for _, v := range doc {
				ch <- &v
			}
			idx = 0
		}
	}

}

func (wdl *WikiDocLoader) ErrExit(err error) {

	wdl.st <- struct{}{}
}

type WikiAbstractIndexBuilder struct {
	f string
}

func NewWikiAbstractIndexBuilder(field string) *WikiAbstractIndexBuilder {
	return &WikiAbstractIndexBuilder{
		f: field,
	}
}

func (wa *WikiAbstractIndexBuilder) Build(doc types.Document, tokens []types.TokenMeta) (res []types.IndexMeta) {
	switch wa.f {
	case "Title":
		for _, v := range tokens {
			kmap := make(map[int64]int16)
			// xid, _ := strconv.ParseInt(doc.UUID(), 10, 64)
			kmap[doc.UUID()] = 1
			res = append(res, types.IndexMeta{
				Token: v.Token(),
				Zindex: &WikiAbstractIndex{
					Token: v.Token(),
					Docs:  kmap,
				},
			})
		}
	default:
		return nil
	}

	return
}

func (wa WikiAbstractIndexBuilder) ErrExit(err error) {
	common.DFAIL("Build Index Target Error %v", err)
}
