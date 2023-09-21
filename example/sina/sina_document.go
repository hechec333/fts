package main

import (
	"bytes"
	"encoding/gob"
	"fts/internal/common"
)

type SinaDocument struct {
	Catgory string `xml:"catgory"`
	Title   string `xml:"title"`
	Content string `xml:"content"`
}

// Implement For Document
func (sd *SinaDocument) Dump(b []byte) {
	d := gob.NewDecoder(bytes.NewReader(b))
	d.Decode(sd)
}

func (sd *SinaDocument) meta() string {
	return "Sina"
}

func (sd *SinaDocument) Serial() []byte {
	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)

	e.Encode(sd)

	return buf.Bytes()
}

func (sd *SinaDocument) UUID() int64 {
	id := common.MergeString(sd.meta(), sd.Catgory, sd.Title)
	return common.StringHashToInt64(id)

}
func (sd *SinaDocument) FieldExist(f string) bool {
	switch f {
	case "Catgory", "Title", "Content":
		goto l
	default:
		return false
	}

l:
	return true
}

func (sd *SinaDocument) FieldLen(f string) int64 {
	switch f {
	case "Catgory":
		return int64(len(sd.Catgory))
	case "Title":
		return int64(len(sd.Title))
	case "Content":
		return int64(len(sd.Content))
	default:
		return -1
	}
}

func (sd *SinaDocument) FetchField(f string) []byte {
	switch f {
	case "Catgory":
		return []byte(sd.Catgory)
	case "Title":
		return []byte(sd.Title)
	case "Content":
		return []byte(sd.Content)
	default:
		return nil
	}
}

func (sd *SinaDocument) EnumFields() []string {
	return []string{"Catgory", "Ttile", "Content"}
}
