package types

import (
	"io"
)

type Cache interface {
	Get(string) (interface{}, bool)
	Put(string, interface{})
	Len() int
	Clear()
}

type ZCache interface {
	RGet(string) (interface{}, bool)
	WGet(string) (interface{}, bool)
	Put(string, interface{})
	Len() int
	Clear()
}

type TokenMeta interface {
	Token() string
	SetToken(string)
	GetMeta(interface{}) interface{}
	SetMeta(interface{}, interface{})
	Copy() TokenMeta //?
}
type Tokenizer interface {
	Analyze(string) []TokenMeta
	UseSegmentor(Segmentor)
	UseFilter(Filter)
}
type Segmentor interface {
	Cut(text string) []TokenMeta
}

type Filter interface {
	Gen([]TokenMeta) []TokenMeta
}

type DiskWriter interface {
	WriteAt(int64, []byte) int64
}
type DiskReader interface {
	ReadAt(int64, []byte) int64
}

type Serializer interface {
	Serial() []byte
	Dump([]byte)
}

type Document interface {
	Serializer
	// UUID() string
	UUID() int64
	FieldExist(string) bool
	FieldLen(string) int64
	FetchField(string) []byte
}

type IndexQueryResult struct {
	Ids  []int64         //有序数组
	Info map[int64]int16 //具体信息
}
type Index interface {
	Serializer
	Field() string
	UUID() int64
	Merge(interface{}) bool
	QueryDoc(int64) int16 //查询docid是否在这个索引中，返回出现次数
	QueryAllDoc() IndexQueryResult
}

type IndexDiskManager interface {
	EnumFields() []string
	GetIndex(int64, string) Index //id,filed
	AddIndex(Index)
	Close() //notify background task to exit
	SaveMeta()
}
type DocID struct {
	DocType string
	ID      int64
}

type DocDiskManager interface {
	GetDoc(int64) Document
	AddDoc(Document) //反复添加，覆盖
	EnumDocTypes() []Document
	EnumDocsID(Document, int) chan int64
	Docs(Document) int64
	Flush()
	SaveMeta()
}
type DocumentLoader interface {
	Load(chan Document, chan error)
	ErrExit(error)
}

type IndexManager interface {
	GetIndex(string, string) Index
	AddIndex(string, Index)
}

type IndexMeta struct {
	Token  string
	Zindex Index
}
type IndexBuilder interface {
	Tokenizer
	ErrExit(error)
	Build(Document, []TokenMeta) []IndexMeta
}

type DiskCodec interface {
	BindR(io.Reader)
	BingW(io.Writer)
	Encode([]byte) (int64, error)
	Decode() ([]byte, error)
}

type QueryLevel uint8

const (
	AT_LEAST = iota // OR NOT
	AT_OR           // common union set
	AT_AND          // common min set
)

// 一个元组形成的查询结果
type QueryReuslt struct {
	Docs   []int64 // open doc id array
	Tokens string  // "beijing&tibet" "beijing|tibet"
}

type Queryer interface {
	Query(string, QueryLevel, ...any) ([]QueryReuslt, map[string]Pair, []int64, string, []string) // result, total doc, tag
	SetIndexManager(IndexManager)
}

type Pair struct {
	Maps map[int64]int16
}
type RankResult struct {
	Token  string
	Doc    []Document
	Scores float64
}
type Ranker interface {
	Rank(string, map[string][]Document, map[string]Pair, map[int64]Document, string) []RankResult //
}
