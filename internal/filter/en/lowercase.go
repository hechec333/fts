package en

import (
	"fts/internal/common"
	"fts/internal/types"
	"strings"
)

type LowercaseFilter struct {
}

func (LowercaseFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	r := common.CopyTokenMetaArray(tokens)
	for i, token := range tokens {
		r[i].SetToken(strings.ToLower(token.Token()))
	}
	return r
}
