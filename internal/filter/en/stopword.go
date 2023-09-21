package en

import (
	"fts/internal/filter/dic"
	"fts/internal/types"
)

// var stopwords = map[string]struct{}{
// 	"a": {}, "and": {}, "be": {}, "have": {}, "i": {}, "has": {},
// 	"not": {}, "for": {}, "on": {}, "with": {}, "he": {}, "as": {}, "she": {},
// 	"you": {}, "at": {}, "this": {}, "but": {}, "by": {}, "form": {},
// 	"in": {}, "of": {}, "that": {}, "the": {}, "to": {},
// }

type StopWordFilter struct {
}

func (StopWordFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	en := dic.LoadDic("en")
	r := make([]types.TokenMeta, 0)
	for _, token := range tokens {
		if !en.TestWords(token.Token()) {
			r = append(r, token)
		}
	}
	return r
}
