package internal

import (
	"bytes"
	"encoding/gob"
	"sync"
	"unicode/utf8"
)

type Trie struct {
	root *TrieNode
	sync.RWMutex
}

type TrieNode struct {
	sync.RWMutex
	ch     rune
	childs []*TrieNode
	ref    int
	end    bool
	data   interface{}
}

func (t *TrieNode) serailNode(s []*serialNode) int {
	t.RLock()
	defer t.RUnlock()
	r := serialNode{
		Ch:     t.ch,
		Ref:    t.ref,
		End:    t.end,
		Data:   t.data,
		Childs: make([]int, len(t.childs)),
	}
	idx := len(s)
	s = append(s, &r)
	for i, v := range t.childs {
		r.Childs[i] = v.serailNode(s)
	}
	return idx
}
func (t *TrieNode) dumpNode(s []*serialNode, idx int) *TrieNode {
	t.ch = s[idx].Ch
	t.data = s[idx].Data
	t.end = s[idx].End
	t.ref = s[idx].Ref
	for _, v := range s[idx].Childs {
		node := &TrieNode{}
		t.childs = append(t.childs, node.dumpNode(s, v))
	}

	return t
}

type Tuple struct {
	key  string
	data interface{}
}

type serialNode struct {
	Ch     rune
	Ref    int
	End    bool
	Data   interface{}
	Childs []int
}

func NewTrie() *Trie {
	return &Trie{
		root: &TrieNode{
			ref:    0,
			childs: make([]*TrieNode, 0),
			end:    false,
		},
	}
}

func (t *Trie) rlock(path []rune) (int, *TrieNode) {
	p := t.root
	p.RLock()
	dep := 0
	lens := len(path)
	for _, r := range path {
		ori := dep
		for _, v := range p.childs {
			if v.ch == r {
				v.RLock()
				p = v
				dep++
				break
			}
		}
		if dep == ori && dep != lens {
			// 如果锁定失败，则退还
			t.rUnlock(path[:dep], true)
			return dep, nil
		}
	}
	return dep, p
}

func (t *Trie) rUnlock(path []rune, lock bool) {
	if !lock {
		return
	}
	p := t.root
	stack := []*TrieNode{p}
	for _, r := range path {
		for _, v := range p.childs {
			if v.ch == r {
				stack = append(stack, v)
				p = v
				break
			}
		}
	}
	lens := len(stack)
	for i := lens - 1; i >= 0; i-- {
		stack[i].RUnlock()
	}
}
func (t *Trie) Serial() []byte {
	sl := make([]*serialNode, 0)

	t.root.serailNode(sl)
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)

	e.Encode(sl)

	return buf.Bytes()
}

func (t *Trie) Dump(b []byte) {
	d := gob.NewDecoder(bytes.NewReader(b))

	sl := make([]*serialNode, 0)

	d.Decode(&sl)

	t.root.dumpNode(sl, 0)
}
func (t *Trie) Len() int {
	if t.root == nil {
		return -1
	}

	t.root.RLock()
	defer t.root.RUnlock()

	return t.root.ref
}
func (t *Trie) Insert(key string, in interface{}) {
	dep := utf8.RuneCountInString(key) - 1
	t.root.Lock()
	t.root.ref++
	t.root.Unlock()
	p := t.root
	for i, r := range key {
		found := false
		p.RLock()
		for _, v := range p.childs {
			if r == v.ch {
				v.Lock()
				v.ref++
				if dep == i {
					v.end = true
					v.data = in
				}
				p.RUnlock()
				found = true
				p = v
				v.Unlock()
				break
			}
		}
		if !found {
			s := &TrieNode{
				ch:     r,
				childs: make([]*TrieNode, 0),
				ref:    1,
			}
			if dep == i {
				s.end = true
				s.data = in
			}
			p.RUnlock()
			p.Lock()
			p.childs = append(p.childs, s)
			p.Unlock()
			p = s
		}
	}
}

func (t *Trie) Search(key string) interface{} {
	// if t.root == nil {
	// 	return nil, false
	// }
	zlen := utf8.RuneCountInString(key)
	lens, tr := t.rlock([]rune(key))
	unlock := zlen == lens
	defer t.rUnlock([]rune(key), unlock)
	if !tr.end {
		return nil
	}
	return tr.data
}

func (t *Trie) StartWith(prefix string) []Tuple {
	if t.root == nil {
		return nil
	}

	zlen := utf8.RuneCountInString(prefix)
	lens, tr := t.rlock([]rune(prefix))
	unlock := zlen == lens
	defer t.rUnlock([]rune(prefix), unlock)
	if !unlock {
		return nil
	}
	tr.RUnlock()
	defer tr.RLock()
	// tuples := []Tuple{}
	return bfs(tr, prefix)
}
func bfs(root *TrieNode, prefix string) []Tuple {
	root.RLock()
	res := []Tuple{}
	for _, v := range root.childs {
		if v.end {
			res = append(res, Tuple{
				key:  prefix + string(v.ch),
				data: v.data,
			})
		}
		z := bfs(v, prefix+string(v.ch))
		if len(z) != 0 {
			res = append(res, z...)
		}
	}
	root.RUnlock()
	return res
}

func (t *Trie) Delete(key string) {
	// if t.root == nil {
	// 	return
	// }
	paths := []*TrieNode{}
	pv := t.root
	pv.RLock()
	dep := 0
	lens := utf8.RuneCountInString(key) - 1
	for _, r := range key {
		ori := dep
		for _, v := range pv.childs {
			if v.ch == r {
				v.RLock()
				paths = append(paths, v)
				pv = v
				dep++
				break
			}
		}
		if ori == dep && dep != lens {
			// string not exist
			return
		}
	}
	x := -1
	for idx, v := range paths {
		v.RUnlock()
		v.Lock()
		if v.ref == 1 {
			x = idx
			v.Unlock()
			break
		} else {
			v.ref--
			if idx == len(paths)-1 {
				v.end = false
			}
		}
		v.Unlock()
	}
	// fast delete
	if x != -1 {
		anchor := paths[x]
		anchor.childs = nil // reset after node
	}

}
