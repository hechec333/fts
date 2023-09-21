package internal

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTrieInsert(t *testing.T) {

	tr := NewTrie()

	tr.Insert("apple", 1)
	tr.Insert("app", 2)
	tr.Insert("as", 3)
	tr.Insert("ass", 4)
	tr.Insert("axp", 4)

}

func TestParrelInsert(t *testing.T) {
	tr := NewTrie()

	words := []string{"apple", "app", "as", "ass", "axp",
		"athree", "atxsa", "bxas", "b", "abxs"}
	t.Logf("Trie Expect %v elems", len(words))
	wg := sync.WaitGroup{}
	cores := 5
	wg.Add(5)
	for i := 0; i < cores; i++ {
		go func(seq int) {
			time.Sleep(1 * time.Second)
			for i := seq; i < len(words); i += cores {
				tr.Insert(words[i], i)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	t.Logf("Trie contains %v elem ", tr.Len())
}

func TestSearch(t *testing.T) {
	tr := NewTrie()

	tr.Insert("apple", 1)
	tr.Insert("app", 2)
	tr.Insert("as", 3)
	tr.Insert("ass", 4)
	tr.Insert("axp", 4)

	assert.Equal(t, 1, tr.Search("apple"))
	assert.Equal(t, 2, tr.Search("app"))
	assert.Equal(t, nil, tr.Search("ap"))
}

func TestPrefixSearch(t *testing.T) {
	tr := NewTrie()

	tr.Insert("apple", 1)
	tr.Insert("app", 2)
	tr.Insert("as", 3)
	tr.Insert("ass", 4)
	tr.Insert("axp", 4)

	tuple1 := tr.StartWith("ap")
	t.Log(tuple1)
	tuple2 := tr.StartWith("a")
	t.Log(tuple2)
}
