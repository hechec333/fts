package en

import (
	"fts/internal/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestMetaTokenImpl struct {
	token string
	metas map[interface{}]interface{}
}

func NewTestMetaToken(token string) *TestMetaTokenImpl {
	return &TestMetaTokenImpl{
		token: token,
		metas: make(map[interface{}]interface{}),
	}
}

func (t *TestMetaTokenImpl) Token() string {
	return t.token
}
func (t *TestMetaTokenImpl) SetToken(s string) {
	t.token = s
}
func (t *TestMetaTokenImpl) GetMeta(key interface{}) interface{} {
	return t.metas[key]
}
func (t *TestMetaTokenImpl) SetMeta(key, val interface{}) {
	t.metas[key] = val
}
func (t *TestMetaTokenImpl) Copy() types.TokenMeta {
	tt := NewTestMetaToken(t.token)

	for k, v := range t.metas {
		tt.metas[k] = v
	}

	return tt
}

func TestLowercase(t *testing.T) {
	in := []types.TokenMeta{
		NewTestMetaToken("HELLO!"),
		NewTestMetaToken("THIS"),
		NewTestMetaToken("Is"),
		NewTestMetaToken("My"),
		NewTestMetaToken("Dog"),
	}

	f := LowercaseFilter{}
	out := []string{"hello!", "this", "is", "my", "dog"}
	tokens := f.Gen(in)

	for i := range tokens {
		assert.Equal(t, out[i], tokens[i].Token())
	}
}

func TestStem(t *testing.T) {
	var (
		in = []types.TokenMeta{
			NewTestMetaToken("cat"),
			NewTestMetaToken("cats"),
			NewTestMetaToken("fish"),
			NewTestMetaToken("fishing"),
			NewTestMetaToken("fished"),
			NewTestMetaToken("airline"),
		}
		out = []string{
			"cat",
			"cat",
			"fish",
			"fish",
			"fish",
			"airlin",
		}
	)

	f := &StemmerFilter{}

	tokens := f.Gen(in)

	for i := range tokens {
		assert.Equal(t, out[i], tokens[i].Token())
	}
}

func TestStopWord(t *testing.T) {
	var (
		in = []types.TokenMeta{
			NewTestMetaToken("i"),
			NewTestMetaToken("am"),
			NewTestMetaToken("the"),
			NewTestMetaToken("cat"),
		}
		out = []string{"am", "cat"}
	)

	f := StopWordFilter{}

	tokens := f.Gen(in)

	for i := range tokens {
		assert.Equal(t, out[i], tokens[i].Token())
	}
}

func TestNouns(t *testing.T) {
	var (
		in = []types.TokenMeta{
			NewTestMetaToken("A"),
			NewTestMetaToken("nouns"),
			NewTestMetaToken("is"),
			NewTestMetaToken("a"),
			NewTestMetaToken("word"),
		}

		out = []string{"nouns", "word"}
	)

	f := NounsFilter{}

	tokens := f.Gen(in)

	for i := range tokens {
		assert.Equal(t, out[i], tokens[i].Token())
	}
}
