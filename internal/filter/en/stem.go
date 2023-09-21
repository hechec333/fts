package en

import (
	"fts/internal/common"
	"fts/internal/types"

	snowballeng "github.com/kljensen/snowball/english"
)

type StemmerFilter struct {
}

func (StemmerFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	r := common.CopyTokenMetaArray(tokens)
	for i, token := range tokens {
		t := snowballeng.Stem(token.Token(), false)
		r[i].SetToken(t)
	}
	return r
}
