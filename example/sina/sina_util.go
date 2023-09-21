package main

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fts/internal/common"
	"fts/internal/tokenizer"
	"fts/internal/types"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

//	type DocumentLoader interface {
//		Load(chan Document, chan error)
//		ErrExit(error)
//	}
//
// implement DocumentLoader
type TxtSinaDocLoader struct {
	p string
}

func NewTxtSinaDocLoader(target string) *TxtSinaDocLoader {
	return &TxtSinaDocLoader{
		p: target,
	}
}

func (sdl *TxtSinaDocLoader) TransferDocToXml(out string) error {
	var (
		max   int64               = 4096
		ch                        = make(chan types.Document, max)
		Errch                     = make(chan error)
		outs  map[string]*os.File = make(map[string]*os.File)
		err   error               = nil
		f     *os.File
	)

	if err != nil {
		return err
	}
	go sdl.Load(ch, Errch)
	for {
		select {
		case err = <-Errch:
			if err != nil {
				sdl.ErrExit(err)
			}
			goto r
		case doc, ret := <-ch:
			if !ret {
				goto r
			}
			var ok bool
			dou := string(doc.FetchField("Catgory"))

			f, ok = outs[dou]
			if !ok {
				f, _ = os.OpenFile(out+"/"+dou+".xml", os.O_CREATE, 0777)
				outs[dou] = f
			}
			e := xml.NewEncoder(f)

			e.Encode(doc)
		}
	}
r:
	for _, v := range outs {
		v.Close()
	}

	return nil
}

func (sdl *TxtSinaDocLoader) Load(chd chan types.Document, che chan error) {

	cnNews := make([]string, 0)
	var (
		l   *os.File
		err error
	)
	dir, err := os.ReadDir(sdl.p)
	if err != nil {
		che <- err
	}

	for _, v := range dir {
		if v.IsDir() {
			cnNews = append(cnNews, sdl.p+"/"+v.Name())
		}
	}
	//che <- errors.New("skip")
	common.DINFO("Loading %v Targets,", len(cnNews))
	common.DINFO("Loading Targets %v", cnNews)
	time.Sleep(time.Second)

	var (
		total   int64 = 0
		targets int64 = 0
		tt            = time.Now()
	)

	defer func() {
		in := recover()
		if in != nil {
			err = in.(error)
			if err != nil {
				common.DFAIL("Loading [%v/%v] Groups,Loading %v Files,Total Cose %v", targets, len(cnNews), total, time.Since(tt))
				che <- err
			}
		}
		common.DINFO("Loading [%v/%v] Groups,Loding %v Files,Total Cose %v", targets, len(cnNews), total, time.Since(tt))
	}()

	for _, v := range cnNews {
		common.DINFO("Loading %v", path.Base(v))
		time.Sleep(time.Second)
		count := 0
		t := time.Now()
		err = filepath.Walk(v, func(pathx string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.IsDir() {
				l, err = os.Open(pathx)
				if err != nil {
					che <- err
					return err
				}
				b, err := io.ReadAll(l)
				if err != nil {
					che <- err
					return err
				}
				bio := bufio.NewReader(bytes.NewReader(b))
				rp := strings.ReplaceAll(v, "\\", "/")
				title, _, _ := bio.ReadLine()
				reb := len(title)
				doc := &SinaDocument{
					Title:   string(title),
					Catgory: path.Base(rp),
					Content: string(b[reb:]), // +1 means pass "\r\n" or "\n"
				}
				count++
				chd <- doc

				l.Close()
			}

			return nil
		})

		if err != nil {
			che <- err
			return
		}
		total += int64(count)
		targets++
		common.DINFO("Loading Target %v Files %v,Cost %v", path.Base(v), count, time.Since(t))
	}

	close(chd)
}

func (sdl *TxtSinaDocLoader) ErrExit(err error) {
	common.DINFO("Loading Document Error %v", err)
}

type SinaIndexBuilder struct {
	types.Tokenizer
	field string
}

func NewSinaIndexBuilder(field string) *SinaIndexBuilder {
	return &SinaIndexBuilder{
		Tokenizer: &tokenizer.ZhTokenizer{},
		field:     field,
	}
}

func (sid *SinaIndexBuilder) ErrExit(err error) {
	common.DFAIL("Building Field %v index Error %v", sid.field, err)
}

func (sid *SinaIndexBuilder) Build(doc types.Document, tokens []types.TokenMeta) (res []types.IndexMeta) {
	switch sid.field {
	case "Title":
		for _, v := range tokens {
			kmap := make(map[int64]int16)
			kmap[doc.UUID()] = 1
			res = append(res, types.IndexMeta{
				Token: v.Token(),
				Zindex: &SinaTitleIndex{
					Token: v.Token(),
					Maps:  kmap,
				},
			})
		}
	default:
		return nil
	}
	return
}
