package tokenizer

import (
	"fts/internal/filter/cn"
	"fts/internal/filter/en"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yanyiwu/gojieba"
)

func TestGojieba(t *testing.T) {
	var s string
	var words []string
	//use_hmm := true
	x := gojieba.NewJieba()
	defer x.Free()
	tz := time.Now()
	s = "据每日人物报道，一份《检举税收违法行为受理回执》显示"
	words = x.CutForSearch(s, true)
	t.Log(s)
	t.Logf("全模式: %v", strings.Join(words, "/"))
	t.Logf("Cost Time %v", time.Since(tz))
}

func TestTag(t *testing.T) {
	jieba := gojieba.NewJieba()
	defer jieba.Free()

	sentence := "李白是一个诗人"
	words := jieba.Tag(sentence)
	t.Log(sentence)
	t.Log("词性标注:", words)
}
func TestZhTokenizer(t *testing.T) {
	f, _ := os.Open("zh.txt")
	defer f.Close()
	// b, _ := io.ReadAll(f)
	// t.Logf("raw text %v", string(b))
	zh := ZhTokenizer{}
	zh.UseFilter(&cn.JiebaNounsFilter{})

	b, err := io.ReadAll(f)
	if err != nil {
		t.Fail()
	}
	zz := zh.Analyze(string(b))

	for _, v := range zz {
		t.Logf("%v", v.Token())
	}
}

func TestEnTokenizer(t *testing.T) {
	f, _ := os.Open("en.txt")
	defer f.Close()

	enz := Tokenizer{}

	enz.UseFilter(en.LowercaseFilter{})
	enz.UseFilter(en.StopWordFilter{})
	enz.UseFilter(en.NounsFilter{})
	//enz.UseFilter(en.StemmerFilter{})
	b, err := io.ReadAll(f)

	if err != nil {
		panic(err)
	}

	enzz := enz.Analyze(string(b))

	for _, v := range enzz {
		t.Logf("%v", v.Token())
	}
}
