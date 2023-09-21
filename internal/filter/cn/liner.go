package cn

import (
	"fts/internal/types"
	"strings"
)

var seg = []rune{'，', '。', '？', '！', '；', '【', '】', '《', '》',
	'‘', '’', '[', ']', '<', '>', '/', '-', '=', '\\', '*', '$', '%', '?', '（', '）', ' '}

func init() {
	loadPauseWord()
}

type LineFilter struct {
}

func (s *LineFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	result := []types.TokenMeta{}

	for _, v := range tokens {
		lines := strings.FieldsFunc(v.Token(), func(r rune) bool {
			for _, v := range seg {
				if r == v {
					return true
				}
			}
			return false
		})
		for _, vv := range lines {
			r := v.Copy()
			if vv == "" {
				continue
			}
			r.SetToken(vv)
			result = append(result, r)
		}
	}
	return result
}
