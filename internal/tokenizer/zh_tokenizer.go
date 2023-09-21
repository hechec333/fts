package tokenizer

import (
	"fts/internal/filter/cn"
	"fts/internal/types"
	"strings"
)

type ZhToken struct {
	zs string
}

func (z *ZhToken) Token() string {
	return z.zs
}

func (z *ZhToken) SetToken(s string) {
	z.zs = s
}
func (z *ZhToken) SetMeta(interface{}, interface{}) {

}

func (z *ZhToken) GetMeta(interface{}) interface{} {
	return nil
}
func (z *ZhToken) Copy() types.TokenMeta {
	zz := *z

	return &zz
}

type ZhTokenizer struct {
	f   []types.Filter
	seg types.Segmentor
}

func (z *ZhTokenizer) UseSegmentor(seg types.Segmentor) {
	z.seg = seg
}

func (z *ZhTokenizer) UseFilter(f types.Filter) {
	z.f = append(z.f, f)
}

func (z *ZhTokenizer) Analyze(text string) []types.TokenMeta {
	if z.seg == nil {
		z.f = append([]types.Filter{&cn.StopWordFilter{}}, z.f...)
		z.f = append([]types.Filter{&cn.LineFilter{}}, z.f...)
	}

	var tokens []types.TokenMeta
	if z.seg == nil {
		pars := strings.Split(text, "\r\n")

		for _, v := range pars {
			tokens = append(tokens, &ZhToken{
				zs: v,
			})
		}
	} else {
		tokens = append(tokens, z.seg.Cut(text)...)
	}

	for _, f := range z.f {
		tokens = f.Gen(tokens)
	}

	return tokens
}
