package main

import (
	"bytes"
	"encoding/gob"
	"fts/internal/common"
	"unicode/utf8"
)

// <title>Wikipedia: Kit-Cat Klock</title>
// <url>https://en.wikipedia.org/wiki/Kit-Cat_Klock</url>
// <abstract>The Kit-Cat Klock is an art deco novelty wall clock shaped like a grinning cat with cartoon eyes that swivel in time with its pendulum tail.</abstract>

type Sublink struct {
	LinkType string `xml:"linktype,attr"`
	Anchor   string `xml:"anchor"`
	Link     string `xml:"link"`
}

// document represents a Wikipedia abstract dump document.
type Document struct {
	Title    string    `xml:"title"`
	URL      string    `xml:"url"`
	Text     string    `xml:"abstract"`
	Sublinks []Sublink `xml:"links>sublink"`
}

func (doc *Document) meta() string {
	return "wiki"
}

func (doc *Document) Dump(b []byte) {
	d := gob.NewDecoder(bytes.NewReader(b))
	d.Decode(doc)
}

func (doc *Document) Serial() []byte {
	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)

	e.Encode(doc)
	return buf.Bytes()
}

func (doc *Document) UUID() int64 {
	xid := common.MergeString(doc.meta(), doc.Title, doc.URL)
	id := common.StringHashToInt64(xid)

	return id
}
func (doc *Document) EnumFields(s string) string {
	switch s {
	case "Title":
		return doc.Title
	case "Url":
		return doc.URL
	case "Abstract":
		return doc.Text
	default:
		return ""
	}
}

func (doc *Document) FetchField(s string) []byte {
	switch s {
	case "Title":
		return []byte(doc.Title)
	case "Url":
		return []byte(doc.URL)
	case "Abstract":
		return []byte(doc.Text)
	default:
		return []byte("")
	}
}

func (doc *Document) FieldExist(s string) bool {
	switch s {
	case "Title", "Url", "Abstract":
		return true
	default:
		return false
	}
}
func (doc *Document) FieldLen(s string) int64 {
	b := doc.FetchField(s)

	if len(b) == 0 {
		return -1
	}

	return int64(utf8.RuneCount(b))
}
