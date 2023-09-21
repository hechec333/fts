package indexer

import (
	"fmt"
	"fts/internal/types"
	"sync"
)

// 主要管理Document 构建倒排索引的过程。由于一般耗时较长，有故障风险。
// 注意持久化构建进度，所有针对同一版本的Document的以构建索引，不会再重启的时候构建
// batch-build，每个batch完成时，记下日志。

type Indexer struct {
	sync.Mutex
	builder types.IndexBuilder
}

func NewIndexer(tzr types.IndexBuilder) *Indexer {
	idr := &Indexer{
		builder: tzr,
	}
	return idr
}

func (idr *Indexer) Build(doc types.Document, fields string) ([]types.IndexMeta, error) {
	text := doc.FetchField(fields)
	if text == nil {
		return nil, fmt.Errorf("no fields %v", fields)
	}

	tokens := idr.builder.Analyze(string(text))
	return idr.builder.Build(doc, tokens), nil
}

func (idr *Indexer) BatchBuild(
	cores int,
	docs []types.Document,
	field string,
	ch chan buildResult,
	complete chan struct{},
) {

	for i := 0; i < cores; i++ {
		go func(seq int) {
			for j := seq; j < len(docs); j += cores {
				indexes, err := idr.Build(docs[seq], field)

				br := buildResult{
					idx:     j,
					indexes: make(map[string]types.Index),
					err:     err,
				}

				for _, v := range indexes {
					br.indexes[v.Token] = v.Zindex
				}
				ch <- br
			}
			complete <- struct{}{}
		}(i)
	}
}
