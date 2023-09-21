package internal

import (
	"bytes"
	"encoding/gob"
	"fts/internal/common"
	"sync"
	"unicode/utf8"
)

type RadixTree struct {
	sync.RWMutex
	root *radixNode
	lens int
}
type Pair struct {
	key  string
	data interface{}
}
type radixNode struct {
	sync.RWMutex
	childs []*radixNode
	key    string
	end    bool
	ref    int
	data   interface{}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func min0(a, b int) int {
	if a > b {
		return max(b, 0)
	}
	return max(a, 0)
}
func (rn *radixNode) remove(node *radixNode) {
	for idx, v := range rn.childs {
		if v == node {
			rn.childs = append(rn.childs[:idx], rn.childs[idx+1:]...)
			break
		}
	}
}
func (rn *radixNode) splitChilds(idx int) {

	node := &radixNode{
		childs: rn.childs,
		key:    rn.key[idx:],
		ref:    rn.ref,
		end:    rn.end,
		data:   rn.data,
	}

	rn.key = rn.key[:idx]
	rn.data = nil
	rn.childs = []*radixNode{node}
	rn.end = false
}

type RadixSerialNode struct {
	Childs []int
	Key    string
	End    bool
	Ref    int
	Data   interface{}
}

func (rn *radixNode) serialNode(s []RadixSerialNode) int {
	rn.RLock()
	defer rn.RUnlock()
	n := RadixSerialNode{
		Key:    rn.key,
		End:    rn.end,
		Ref:    rn.ref,
		Data:   rn.data,
		Childs: make([]int, 0),
	}
	idx := len(s)
	s = append(s, n)
	for _, v := range rn.childs {
		n.Childs = append(n.Childs, v.serialNode(s))
	}
	s[idx] = n //将children的改动变化到s中
	return idx
}

func (rn *radixNode) dumpNode(s []RadixSerialNode, idx int) *radixNode {
	rn.Lock()
	defer rn.Unlock()
	rn.ref = s[idx].Ref
	rn.key = s[idx].Key
	rn.end = s[idx].End
	rn.data = s[idx].Data
	for _, v := range s[idx].Childs {
		node := &radixNode{}
		rn.childs = append(rn.childs, node.dumpNode(s, v))
	}
	return rn
}

func NewRadixTree() *RadixTree {
	return &RadixTree{
		root: &radixNode{
			childs: make([]*radixNode, 0),
			end:    false,
			ref:    0,
		},
		lens: 0,
	}
}

func (t *RadixTree) Serial() []byte {
	sl := make([]RadixSerialNode, 0)
	t.root.serialNode(sl)

	b := new(bytes.Buffer)

	e := gob.NewEncoder(b)

	e.Encode(sl)

	return b.Bytes()
}

func (t *RadixTree) Dump(b []byte) {
	sl := make([]RadixSerialNode, 0)
	d := gob.NewDecoder(bytes.NewReader(b))

	d.Decode(&sl)

	node := &radixNode{}

	node = node.dumpNode(sl, 0)

	// !!ATTTENTION
	t.Lock()
	t.root = node
	t.lens = len(sl)
	t.Unlock()
}

func (t *RadixTree) rlock(path string) *radixNode {
	t.RLock()
	p := t.root
	pos := 0
	p.RLock()
	//lens := len(path)
	for pos < utf8.RuneCountInString(path) {
		found := false
		for _, r := range p.childs {
			//found := false
			r.RLock()
			if idx := common.LongestCommonPrefix([]rune(r.key), []rune(path[pos:])); idx != -1 {
				pos += idx
				p = r
				found = true
				break
			}
			r.RUnlock()
		}
		if !found {
			t.runlock(path, true)
			return nil
		}
	}
	return p
}

func (t *RadixTree) runlock(path string, lock bool) {
	if !lock {
		return
	}
	defer t.RUnlock()
	p := t.root
	stack := []*radixNode{p}
	pos := 0
	for pos < utf8.RuneCountInString(path) {
		found := false
		for _, r := range p.childs {
			if idx := common.LongestCommonPrefix([]rune(r.key), []rune(path[pos:])); idx != -1 {
				stack = append(stack, r)
				p = r
				pos += idx
				found = true
				break
			}
		}
		if !found {
			return
		}
	}
	lens := len(stack)
	for i := lens - 1; i >= 0; i-- {
		stack[i].RUnlock()
	}
}

func (rt *RadixTree) Insert(key string, in interface{}) {
	pv := rt.root
	pos := 0
	lens := utf8.RuneCountInString(key)
	for pos < lens {
		found := false
		pv.RLock()
		for _, r := range pv.childs {
			r.RLock()
			if idx := common.LongestCommonPrefix([]rune(r.key), []rune(key[pos:])); idx != -1 {
				found = true
				r.RUnlock()
				pv.RUnlock()
				pv.Lock()
				pv.ref++
				pv.Unlock()

				r.Lock()
				// "rxpp" "rxp"
				if idx+pos == lens {
					if r.key != key[pos:] {
						r.splitChilds(idx)
					}
					r.end = true
					r.data = in
					pos++
					r.Unlock()
					break
				}
				// "ra" "ra"
				if r.key == key[pos:] {
					r.end = true
					r.data = in
					pos++
					r.Unlock()
					break
				}
				// "rxa" "rxp"
				if idx != len(r.key) {
					r.splitChilds(idx)
				}
				pv = r
				pos += idx
				r.Unlock()
				break
			} else {
				r.RUnlock()
			}
		}
		if !found {
			pv.RUnlock()
			pv.Lock()
			node := &radixNode{
				childs: make([]*radixNode, 0),
				key:    key[pos:],
				end:    true,
				ref:    1,
				data:   in,
			}
			pv.ref++
			pv.childs = append(pv.childs, node)
			pv.Unlock()
			pos = lens
		}
	}

	rt.Lock()
	rt.lens++
	rt.Unlock()
}

func (rt *RadixTree) Search(key string) (interface{}, bool) {
	//pv := rt.root
	n := rt.rlock(key)
	defer rt.runlock(key, n != nil)

	if n == nil {
		return nil, false
	}
	return n.data, true
}

func (rt *RadixTree) StartWith(prefix string) []Pair {
	n := rt.rlock(prefix)
	defer rt.runlock(prefix, n != nil)

	if n == nil {
		return nil
	}

	n.RUnlock()
	defer n.RLock()

	return n.bfsGetPair(prefix)
}

func (root *radixNode) bfsGetPair(prefix string) []Pair {
	root.RLock()
	res := []Pair{}
	for _, v := range root.childs {
		if v.end {
			res = append(res, Pair{
				key:  prefix + v.key,
				data: v.data,
			})
		}
		z := v.bfsGetPair(prefix + v.key)
		if len(z) != 0 {
			res = append(res, z...)
		}
	}
	root.RUnlock()
	return res
}

func (rt *RadixTree) Len() int {
	rt.root.RLock()
	defer rt.root.RUnlock()

	return rt.lens
}
func (rt *RadixTree) Delete(key string) {
	pv := rt.root
	pos := 0
	lens := utf8.RuneCountInString(key) - 1
	stack := []*radixNode{pv}
	for pos <= lens {
		found := false
		pv.RLock()
		for _, r := range pv.childs {
			r.RLock()
			if idx := common.LongestCommonPrefix([]rune(r.key), []rune(key[pos:])); idx != -1 {
				found = true
				stack = append(stack, r)
				pv.RUnlock()
				pv = r
				pos += idx
				r.RUnlock()
				break
			}
			r.RUnlock()
		}
		if !found {
			return
		}
	}

	for i := 1; i < len(stack); i++ {
		stack[i].Lock()
		if stack[i].ref == 1 {
			stack[i-1].remove(stack[i])
			stack[i].Unlock()
			return
		} else {
			stack[i].ref = max(stack[i].ref-1, 0)
		}
		stack[i].Unlock()
	}

}
