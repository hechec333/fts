package tokenizer

import (
	"fts/internal/types"
	"strings"
	"unicode"
)

type PosTagToken struct {
	token string
	pos   int
}

func (nt *PosTagToken) SetToken(text string) {
	nt.token = text
}
func (nt *PosTagToken) Token() string {
	return nt.token
}
func (nt *PosTagToken) Copy() types.TokenMeta {
	return &PosTagToken{
		token: nt.token,
		pos:   nt.pos,
	}
}
func (nt *PosTagToken) SetMeta(key interface{}, i interface{}) {
	if key == "pos" {
		nt.pos = i.(int)
	}
}
func (nt *PosTagToken) GetMeta(key interface{}) interface{} {
	if key == "pos" {
		return nt.pos
	}
	return nil
}

type PosTagTokenizer struct {
	filters []types.Filter
}

func NewPosTagTokenizer() *PosTagTokenizer {
	return &PosTagTokenizer{
		filters: make([]types.Filter, 0),
	}
}
func (ntz *PosTagTokenizer) Analyze(text string) []types.TokenMeta {
	tokens := ntz.analyze(text)
	for _, filter := range ntz.filters {
		tokens = filter.Gen(tokens)
	}

	return tokens
}
func (ntz *PosTagTokenizer) Use(f types.Filter) {
	ntz.filters = append(ntz.filters, f)
}

func (ntz *PosTagTokenizer) analyze(text string) (data []types.TokenMeta) {
	slice := strings.Fields(text)
	for _, v := range slice {
		ret := posTagger([]rune(v))
		if ret != nil {
			data = append(data, ret...)
		}
	}
	return data
}

func posTagger(token []rune) []types.TokenMeta {
	dst := make([]types.TokenMeta, 0)
	pos := 0
	last := 0
	for pos < len(token) {
		v := token[pos]
		if !unicode.IsLetter(v) && !unicode.IsNumber(v) && pos != last {
			dst = append(dst, &PosTagToken{
				token: string(token[last:pos]),
				pos:   last,
			})
			last = pos
		}
		pos++
	}
	return dst
}
