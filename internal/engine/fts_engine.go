package engine

import (
	"errors"
	"fts/internal/common"
	"fts/internal/document"
	"fts/internal/indexer"
	"fts/internal/types"
)

var (
	ErrNotFound = errors.New("not found keys")
)

type Engine struct {
	root    string                    //根目录
	docm    *document.DocumentManager //文档管理器
	indexm  types.IndexManager        //索引管理器
	queryer types.Queryer             //查询器
	indexer indexer.IndexerManager    //索引构建器
	ranker  types.Ranker
}

type QueryResult struct {
	FileRune []types.Document
	Prefix   string
	Token    string
	Field    string
}

func NewFTSEngine(
	root string,
	doc types.DocDiskManager,
	index types.IndexManager,
	queryer types.Queryer,
	ranker types.Ranker,
	builder types.IndexBuilder,
) *Engine {
	eig := &Engine{
		docm:    document.NewDocumentManager(64, doc),
		root:    root,
		indexm:  index,
		queryer: queryer,
		indexer: *indexer.NewIndexerManager(root, builder),
		ranker:  ranker,
	}
	//eig.queryer.Use(ranker)
	eig.queryer.SetIndexManager(eig.indexm)
	eig.indexer.OnBuild(func(bi []indexer.BuildInfo, err error) error {
		if err != nil {
			return err
		}
		for _, v := range bi {
			common.INFO("Document %v,Include %v", v.DocID, v.IndexIDS)
		}
		return nil
	})

	eig.indexer.SetBatchSize(16)

	return eig
}

// *** query ***
// 查询所有token的并集
func (e *Engine) QueryOr(text string, field string) ([]QueryResult, error) {
	result, loadmaps, ids, prefix, _ := e.queryer.Query(text, types.AT_OR)

	if len(result) == 0 {
		return nil, ErrNotFound
	}
	docs := make(map[int64]types.Document)

	for _, v := range ids {
		docs[v] = e.docm.GetDocument(v)
	}
	rxoc := make(map[string][]types.Document)
	for _, v := range result {
		dd := make([]types.Document, len(v.Docs))
		for _, vv := range v.Docs {
			dd = append(dd, docs[vv])
		}
		rxoc[v.Tokens] = dd
	}

	res := e.ranker.Rank(field, rxoc, loadmaps, docs, prefix)

	qr := make([]QueryResult, 0)
	qscore := make([]float64, 0)
	for _, v := range res {
		idx := 0
		for ; idx < len(qr); idx++ {
			if qscore[idx] < v.Scores {
				break
			}
		}
		sl := []types.Document{}
		sl = append(sl, v.Doc...)
		if idx == len(qr) {
			qscore = append(qscore, v.Scores)
			qr = append(qr, QueryResult{
				FileRune: sl,
				Prefix:   prefix,
				Token:    v.Token,
				Field:    field,
			})
		} else {
			qscore = append(qscore[:idx], append([]float64{v.Scores}, qscore[idx+1:]...)...)
			qr = append(qr[:idx], append([]QueryResult{
				{
					FileRune: sl,
					Prefix:   prefix,
					Token:    v.Token,
					Field:    field,
				},
			}, qr[idx+1:]...)...)
		}
	}

	return qr, nil
}

// 查询所有token的交集
func (e *Engine) QueryAnd(text string, field string) ([]QueryResult, error) {
	result, loadmaps, ids, prefix, _ := e.queryer.Query(text, types.AT_AND)

	docs := make(map[int64]types.Document)

	for _, v := range ids {
		docs[v] = e.docm.GetDocument(v)
	}
	rxoc := make(map[string][]types.Document)
	for _, v := range result {
		dd := make([]types.Document, len(v.Docs))
		for _, vv := range v.Docs {
			dd = append(dd, docs[vv])
		}
		rxoc[v.Tokens] = dd
	}

	res := e.ranker.Rank(field, rxoc, loadmaps, docs, prefix)

	qr := make([]QueryResult, 0)
	qscore := make([]float64, 0)
	for _, v := range res {
		idx := 0
		for ; idx < len(qr); idx++ {
			if qscore[idx] < v.Scores {
				break
			}
		}
		sl := []types.Document{}
		sl = append(sl, v.Doc...)
		if idx == len(qr) {
			qscore = append(qscore, v.Scores)
			qr = append(qr, QueryResult{
				FileRune: sl,
				Prefix:   prefix,
				Token:    v.Token,
				Field:    field,
			})
		} else {
			qscore = append(qscore[:idx], append([]float64{v.Scores}, qscore[idx+1:]...)...)
			qr = append(qr[:idx], append([]QueryResult{
				{
					FileRune: sl,
					Prefix:   prefix,
					Token:    v.Token,
					Field:    field,
				},
			}, qr[idx+1:]...)...)
		}
	}

	return qr, nil
}

// 粗粒度token查询，h=查询粒度 归一化参数 h=0.8 h=1.0，仅查询h
func (e *Engine) QueryAndK(text string, field string, k float64) ([]QueryResult, error) {
	if 1.0-k <= 1.0e-6 {
		return e.QueryAnd(text, field)
	}

	result, loadmaps, ids, prefix, _ := e.queryer.Query(text, types.AT_LEAST, false, false, k)

	docs := make(map[int64]types.Document)

	for _, v := range ids {
		docs[v] = e.docm.GetDocument(v)
	}
	rxoc := make(map[string][]types.Document)
	for _, v := range result {
		dd := make([]types.Document, len(v.Docs))
		for _, vv := range v.Docs {
			dd = append(dd, docs[vv])
		}
		rxoc[v.Tokens] = dd
	}

	res := e.ranker.Rank(field, rxoc, loadmaps, docs, prefix)

	qr := make([]QueryResult, 0)
	qscore := make([]float64, 0)
	for _, v := range res {
		idx := 0
		for ; idx < len(qr); idx++ {
			if qscore[idx] < v.Scores {
				break
			}
		}
		sl := []types.Document{}
		sl = append(sl, v.Doc...)
		if idx == len(qr) {
			qscore = append(qscore, v.Scores)
			qr = append(qr, QueryResult{
				FileRune: sl,
				Prefix:   prefix,
				Token:    v.Token,
				Field:    field,
			})
		} else {
			qscore = append(qscore[:idx], append([]float64{v.Scores}, qscore[idx+1:]...)...)
			qr = append(qr[:idx], append([]QueryResult{
				{
					FileRune: sl,
					Prefix:   prefix,
					Token:    v.Token,
					Field:    field,
				},
			}, qr[idx+1:]...)...)
		}
	}

	return qr, nil
}

// 粗粒度查询，h=查询粒度 归一化参数 eg h=0.5 ，查询不少于h
func (e *Engine) QueryAndKLeast(text string, field string, k float64) ([]QueryResult, error) {
	//"|"
	if 1.0-k <= 1.0e-6 {
		return e.QueryOr(text, field)
	}
	result, loadmaps, ids, prefix, _ := e.queryer.Query(text, types.AT_LEAST, true, false, k)

	docs := make(map[int64]types.Document)

	for _, v := range ids {
		docs[v] = e.docm.GetDocument(v)
	}
	rxoc := make(map[string][]types.Document)
	for _, v := range result {
		dd := make([]types.Document, len(v.Docs))
		for _, vv := range v.Docs {
			dd = append(dd, docs[vv])
		}
		rxoc[v.Tokens] = dd
	}

	res := e.ranker.Rank(field, rxoc, loadmaps, docs, prefix)

	qr := make([]QueryResult, 0)
	qscore := make([]float64, 0)
	for _, v := range res {
		idx := 0
		for ; idx < len(qr); idx++ {
			if qscore[idx] < v.Scores {
				break
			}
		}
		sl := []types.Document{}
		sl = append(sl, v.Doc...)
		if idx == len(qr) {
			qscore = append(qscore, v.Scores)
			qr = append(qr, QueryResult{
				FileRune: sl,
				Prefix:   prefix,
				Token:    v.Token,
				Field:    field,
			})
		} else {
			qscore = append(qscore[:idx], append([]float64{v.Scores}, qscore[idx+1:]...)...)
			qr = append(qr[:idx], append([]QueryResult{
				{
					FileRune: sl,
					Prefix:   prefix,
					Token:    v.Token,
					Field:    field,
				},
			}, qr[idx+1:]...)...)
		}
	}

	return qr, nil
}

// 粗粒度查询，h=查询粒度 归一化参数 h=0.8 h=1.0 ,仅查询h
func (e *Engine) QueryOrK(text string, field string, k float64) ([]QueryResult, error) {
	//"|"
	if 1.0-k <= 1.0e-6 {
		return e.QueryOr(text, field)
	}
	result, loadmaps, ids, prefix, _ := e.queryer.Query(text, types.AT_LEAST, false, true, k)

	docs := make(map[int64]types.Document)

	for _, v := range ids {
		docs[v] = e.docm.GetDocument(v)
	}
	rxoc := make(map[string][]types.Document)
	for _, v := range result {
		dd := make([]types.Document, len(v.Docs))
		for _, vv := range v.Docs {
			dd = append(dd, docs[vv])
		}
		rxoc[v.Tokens] = dd
	}

	res := e.ranker.Rank(field, rxoc, loadmaps, docs, prefix)

	qr := make([]QueryResult, 0)
	qscore := make([]float64, 0)
	for _, v := range res {
		idx := 0
		for ; idx < len(qr); idx++ {
			if qscore[idx] < v.Scores {
				break
			}
		}
		sl := []types.Document{}
		sl = append(sl, v.Doc...)
		if idx == len(qr) {
			qscore = append(qscore, v.Scores)
			qr = append(qr, QueryResult{
				FileRune: sl,
				Prefix:   prefix,
				Token:    v.Token,
				Field:    field,
			})
		} else {
			qscore = append(qscore[:idx], append([]float64{v.Scores}, qscore[idx+1:]...)...)
			qr = append(qr[:idx], append([]QueryResult{
				{
					FileRune: sl,
					Prefix:   prefix,
					Token:    v.Token,
					Field:    field,
				},
			}, qr[idx+1:]...)...)
		}
	}

	return qr, nil
}

// 粗粒度查询，h=查询粒度 归一化参数 eg h=0.5，查询不少于h
func (e *Engine) QueryOrKLeast(text string, field string, k float64) ([]QueryResult, error) {
	if 1.0-k <= 1.0e-6 {
		return e.QueryOr(text, field)
	}
	result, loadmaps, ids, prefix, _ := e.queryer.Query(text, types.AT_LEAST, false, true, k)

	docs := make(map[int64]types.Document)

	for _, v := range ids {
		docs[v] = e.docm.GetDocument(v)
	}
	rxoc := make(map[string][]types.Document)
	for _, v := range result {
		dd := make([]types.Document, len(v.Docs))
		for _, vv := range v.Docs {
			dd = append(dd, docs[vv])
		}
		rxoc[v.Tokens] = dd
	}

	res := e.ranker.Rank(field, rxoc, loadmaps, docs, prefix)

	qr := make([]QueryResult, 0)
	qscore := make([]float64, 0)
	for _, v := range res {
		idx := 0
		for ; idx < len(qr); idx++ {
			if qscore[idx] < v.Scores {
				break
			}
		}
		sl := []types.Document{}
		sl = append(sl, v.Doc...)
		if idx == len(qr) {
			qscore = append(qscore, v.Scores)
			qr = append(qr, QueryResult{
				FileRune: sl,
				Prefix:   prefix,
				Token:    v.Token,
				Field:    field,
			})
		} else {
			qscore = append(qscore[:idx], append([]float64{v.Scores}, qscore[idx+1:]...)...)
			qr = append(qr[:idx], append([]QueryResult{
				{
					FileRune: sl,
					Prefix:   prefix,
					Token:    v.Token,
					Field:    field,
				},
			}, qr[idx+1:]...)...)
		}
	}

	return qr, nil
}

// *** load ***

// load document
func (e *Engine) Load(loader types.DocumentLoader) {
	e.docm.LoadDocument(loader)
}

// *** build ***
func (e *Engine) Build(typ types.Document, field string) error {
	return e.indexer.BuildIndex(typ, field, e.docm, e.indexm)
}
