package en

import (
	"fts/internal/types"

	"github.com/jdkato/prose/tag"
)

type NounsFilter struct {
}

func (NounsFilter) Gen(tokens []types.TokenMeta) []types.TokenMeta {
	return NNFilter(tokens)
}

func NNFilter(tokens []types.TokenMeta) []types.TokenMeta {
	r := make([]types.TokenMeta, 0)
	tagger := tag.NewPerceptronTagger()
	words := []string{}
	maps := make(map[string]types.TokenMeta)
	for _, v := range tokens {
		words = append(words, v.Token())
		maps[v.Token()] = v
	}
	tags := tagger.Tag(words)

	for _, token := range tags {
		if token.Tag == "NN" || token.Tag == "NNS" {
			r = append(r, maps[token.Text])
		}
	}
	return r
}
