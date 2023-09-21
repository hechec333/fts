package query

import (
	"fts/internal/types"
	"math"
	"strconv"
	"strings"
)

type BM25Ranker struct {
	k1 float32
	k2 float32
	b  float32
}

func NewBM25Ranker(k1 float32, k2 float32, b float32) *BM25Ranker {
	return &BM25Ranker{
		k1: k1,
		k2: k2,
		b:  b,
	}
}

// 完成BM25算法接口
// l token -> ids
// t token ->
func (bm *BM25Ranker) Rank(
	field string, //字段名
	l map[string][]types.Document, // token列表打开的文档
	t map[string]types.Pair,
	open map[int64]types.Document,
	prefix string,
) []types.RankResult {
	var (
		result = make([]types.RankResult, 0)
	)
	avglen := 0.0
	for _, v := range open {
		avglen += float64(v.FieldLen(field))
	}
	for key, docs := range l {
		//todo
		var (
			N       = len(open)
			scores  = make([]float64, 0)
			doms    = make([]types.Document, 0)
			qscores = 0.0 // 子query在全token中的得分，一般是打开文档的得分均值
			tokens  = strings.Split(key, prefix)
		)
		for _, d := range docs {
			var qdscores = 0.0 // 子query在对于某个文档的相似性评估结果
			for _, v := range tokens {
				var qi int
				if prefix == "&" {
					qi = len(v)
				} else {
					qi = len(t[v].Maps)
				}
				idf := math.Log((float64(N-qi) + 0.5) / (float64(qi) + 0.5))

				K := bm.k1 * (1 - bm.b + bm.b*float32(d.FieldLen(field))/float32(avglen))
				xid, _ := strconv.ParseInt(v, 10, 64)
				fi := float64(t[v].Maps[xid])
				R := fi*float64(bm.k1) + fi/fi + float64(K)
				qdscores += idf * R
			}
			var idx = 0
			for ; idx < len(scores); idx++ {
				if scores[idx] < qdscores {
					break
				}
			}
			if idx != len(scores) {
				scores = append(scores[:idx], append([]float64{qdscores}, scores[idx+1:]...)...)
				doms = append(doms[:idx], append([]types.Document{d}, doms[idx+1:]...)...)
			} else {
				scores = append(scores, qdscores)
				doms = append(doms, d)
			}
		}

		mid := len(scores) / 2
		qscores = scores[mid]

		result = append(result, types.RankResult{
			Token:  key,
			Doc:    doms,
			Scores: qscores,
		})

		qscores = 0.0
	}

	return result
}
