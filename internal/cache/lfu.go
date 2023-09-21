package cache

// TODO

import (
	"container/heap"
)

type cacheItem struct {
	key   string
	value interface{}
	freq  int
	index int
}

type PriorityQueue []*cacheItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].freq < pq[j].freq
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*cacheItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type LFUCache struct {
	capacity int
	items    map[string]*cacheItem
	queue    PriorityQueue
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		capacity: capacity,
		items:    make(map[string]*cacheItem),
		queue:    make(PriorityQueue, 0),
	}
}

func (c *LFUCache) Get(key string) (interface{}, bool) {
	item, ok := c.items[key]
	if !ok {
		return nil, false
	}
	item.freq++
	heap.Fix(&c.queue, item.index)
	return item.value, true
}

func (c *LFUCache) Put(key string, value interface{}) {
	if c.capacity == 0 {
		return
	}
	if item, ok := c.items[key]; ok {
		item.value = value
		item.freq++
		heap.Fix(&c.queue, item.index)
	} else {
		if len(c.items) == c.capacity {
			evicted := heap.Pop(&c.queue).(*cacheItem)
			delete(c.items, evicted.key)
		}
		newItem := &cacheItem{
			key:   key,
			value: value,
			freq:  1,
		}
		c.items[key] = newItem
		heap.Push(&c.queue, newItem)
	}
}

func (c *LFUCache) Len() int {
	return len(c.items)
}

func (c *LFUCache) Clear() {
	c.items = make(map[string]*cacheItem)
	c.queue = make(PriorityQueue, 0)
}
