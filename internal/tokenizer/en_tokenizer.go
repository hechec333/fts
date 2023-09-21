package tokenizer

import (
	"fts/internal/types"
	"strings"
	"unicode"
)

type EnToken struct {
	token string
}

func (z *EnToken) Token() string {
	return z.token
}

func (z *EnToken) SetToken(s string) {
	z.token = s
}

func (z *EnToken) SetMeta(k interface{}, v interface{}) {

}

func (z *EnToken) GetMeta(k interface{}) interface{} {
	return nil
}

func (z *EnToken) Copy() types.TokenMeta {
	return &EnToken{
		token: z.token,
	}
}

type Tokenizer struct {
	filters []types.Filter
	seg     types.Segmentor
}

func (t *Tokenizer) UseSegmentor(seg types.Segmentor) {
	t.seg = seg
}
func (t *Tokenizer) Analyze(text string) []types.TokenMeta {
	var tokens []types.TokenMeta
	if t.seg == nil {
		tokens = t.analyze(text)
	} else {
		tokens = t.seg.Cut(text)
	}

	for _, filter := range t.filters {
		tokens = filter.Gen(tokens)
	}

	return tokens
}

func (t *Tokenizer) UseFilter(f types.Filter) {
	t.filters = append(t.filters, f)
}

func (t *Tokenizer) analyze(text string) (data []types.TokenMeta) {
	slice := strings.Fields(text)
	for _, v := range slice {
		ret := t.tagger([]rune(v))
		if ret != nil {
			data = append(data, ret...)
		}
	}
	return data
}

func (t *Tokenizer) tagger(sl []rune) []types.TokenMeta {
	// sl: "source:xinhua"
	dst := make([]types.TokenMeta, 0)
	pos := 0
	last := 0
	for pos < len(sl) {
		v := sl[pos]
		if !unicode.IsLetter(v) && !unicode.IsNumber(v) && pos != last {
			dst = append(dst, &EnToken{
				token: string(sl[last:pos]),
			})
			last = pos
		}
		pos++
	}
	dst = append(dst, &EnToken{
		token: string(sl[last:pos]),
	})
	return dst
}
