package cn

import (
	"fts/internal/common"
	"fts/internal/filter/cn/py"
	"fts/internal/filter/dic"
	"fts/internal/types"
	"strings"

	"github.com/yanyiwu/gojieba"
)

type PyNounsFilter struct {
}

func pymergeNouns(r []py.TaggerResult) []string {
	idx := 0
	token := []string{}
	for idx < len(r) {
		var t string
		i := idx
		for ; i < len(r)-1; i++ {
			if strings.Contains(r[i].NNS, "n") {
				t += r[i].Tokens
				continue
			}
			break
		}

		if t != "" {
			token = append(token, t)
		}
		if idx == len(r)-1 {
			if strings.Contains(r[idx].NNS, "n") {
				token = append(token, r[idx].Tokens)
			}
		}
		idx = i + 1
	}

	return token
}

func (nf *PyNounsFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {

	token := []types.TokenMeta{}

	for idx, v := range tokens {
		r, err := py.TagText(v.Token())
		if err != nil {
			common.WARN("TagText Fail")
		}
		tos := pymergeNouns(r)

		if len(tos) != 0 {
			for _, v := range tos {
				tt := tokens[idx].Copy()
				tt.SetToken(v)
				token = append(token, tt)
			}
		}
	}

	return token
}

type JiebaNounsFilter struct{}

func (nf *JiebaNounsFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	token := []types.TokenMeta{}
	jieba := gojieba.NewJieba()
	defer jieba.Free()
	for idx, v := range tokens {
		r := jieba.Tag(v.Token())
		rs := []string{}
		ns := []string{}
		for _, v := range r {
			is := strings.Split(v, "/")
			rs = append(rs, is[0])
			ns = append(ns, is[1])
		}
		tos := jiebamergeNouns(rs, ns)
		ttos := nameExtract(rs)

		tos = append(tos, ttos...)
		if len(tos) != 0 {
			for _, v := range tos {
				tt := tokens[idx].Copy()
				tt.SetToken(v)
				token = append(token, tt)
			}
		}
	}

	return token
}

func jiebamergeNouns(r []string, ns []string) []string {
	idx := 0
	token := []string{}
	for idx < len(r) {
		var t string
		i := idx
		for ; i < len(r)-1; i++ {
			if strings.Contains(ns[i], "n") {
				t += r[i]
				continue
			}
			break
		}
		if t != "" {
			token = append(token, t)
		}
		if idx == len(r)-1 {
			if strings.Contains(ns[i], "n") {
				token = append(token, r[i])
			}
		}
		idx = i + 1
	}

	return token
}

func nameExtract(ns []string) (ts []string) {
	xdic := dic.LoadDic("cnname")

	for _, v := range ns {
		if xdic.TestWords(v) {
			ts = append(ts, v)
		}
	}

	return
}
