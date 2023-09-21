package cn

import (
	"fts/internal/filter/dic"
	"fts/internal/types"
)

type StopWordFilter struct {
}

func (swf *StopWordFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	sd := dic.LoadDic("cn")

	token := []types.TokenMeta{}

	for _, v := range tokens {
		if !sd.TestWords(v.Token()) {
			token = append(token, v)
		}
	}
	return token
}
