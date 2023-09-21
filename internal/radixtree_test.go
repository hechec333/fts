package internal

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRadixTree(t *testing.T) {

	tree := NewRadixTree()
	data := map[string]string{
		"ra":     "xx",
		"rax":    "ss",
		"raxid":  "zz",
		"rxaid":  "hh",
		"rxxeq":  "qq",
		"rapp":   "yy",
		"rxxid":  "gg",
		"rkxpp":  "kk",
		"rxxaid": "rr",
	}

	sl := []string{}
	for k := range data {
		sl = append(sl, k)
	}
	cores := 3
	t.Log("cocurrent insert")
	wg := sync.WaitGroup{}
	wg.Add(cores)
	for i := 0; i < cores; i++ {
		go func(seq int) {
			time.Sleep(2 * time.Second) // wait to start
			for start := seq; start < len(sl); start += cores {
				//t.Log(sl[start])
				tree.Insert(sl[start], data[sl[start]])
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	//return
	t.Log("cocurrent search")
	t.Log(tree.Len())
	nums := cores
	seq := make([]int, nums)

	for i := 0; i < nums; i++ {
		seq[i] = rand.Intn(len(data))
	}

	// t.Logf("Search key %v expected %v actual %v", sl[idx], data[sl[idx]], in.(string))
	wg.Add(nums)

	for i := 0; i < nums; i++ {
		go func(q int) {
			time.Sleep(2 * time.Second) // wait for cocurrency
			defer wg.Done()
			in, ok := tree.Search(sl[seq[q]])
			t.Logf("key %v value %v", sl[seq[q]], in)
			assert.Equal(t, true, ok, fmt.Sprintf("not found key %v", sl[seq[q]]))
			if in == nil {
				t.Log("shouldn't receive nil")
				return
			}
			assert.Equal(t, data[sl[seq[q]]], in.(string), fmt.Sprintf("key %v expetecd %v", sl[seq[q]], data[sl[seq[q]]]))
		}(i)
	}

	wg.Wait()
	//delete

	idx := rand.Intn(len(data))
	t.Log("delete key ", sl[idx])
	tree.Delete(sl[idx])
	time.Sleep(time.Second)

	_, ok := tree.Search(sl[idx])

	assert.Equal(t, false, ok, "delete key shouldn't be detected")

}
