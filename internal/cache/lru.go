package cache

import "sync"

// lru cache
// technically requirement :
// 1.O(1) get element use a key
// 2.O(1) to put element use a key
// 3. fix size cache , when
// tips:
//   1. only hashtable can normaly get/put a element in O(1)
//   2. Lru require will adjust the postion of the accessed data
//   3. the data-access position is random ,that means you need to move a data postion to header in O(1)
//   4. O(1) leave the less used data.

//  conclusion:
//     1.base on tips.@1 hashtable is needed,to provide O(1) key indexer.
//     2.accorading to tips.@2-@3 the hashtable couldn't mark the hot key
//     3.bacause the DlinkList could move a data-node in O(1) into the header,and O(1) to leave the tailNode
//     4.Use DlinkList's Header to mark the hot key ,and the Tail to present the Less Recently use data
//     5.Use the compostion of HashTable and DlinkList . Use HashTable to index or map the key to DLinkNode
//       Use the DlinkList to manage the key lifetime.

type EvitedCallback = func(string, interface{})
type MissCallback = func(string) interface{}
type LruCache struct {
	mu       sync.RWMutex
	capacity int64
	onMissed MissCallback
	onEvited EvitedCallback
	Maps     map[string]*LinkedListNode
	dList    *List
}

func Default(cap int64) *LruCache {
	return NewLruCache(cap, nil, nil)
}
func NewLruCache(cap int64, tt MissCallback, callback EvitedCallback) *LruCache {
	return &LruCache{
		capacity: cap,
		Maps:     make(map[string]*LinkedListNode, cap),
		dList:    NewList(),
		onMissed: tt,
		onEvited: callback,
	}
}

func (L *LruCache) AttchMissCaller(caller MissCallback) {
	L.onMissed = caller
}
func (l *LruCache) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.Maps)
}
func (l *LruCache) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.onEvited != nil {
		for k, v := range l.Maps {
			l.onEvited(k, v)
		}
	}
	l.dList = nil
	l.Maps = make(map[string]*LinkedListNode)
}
func (l *LruCache) Get(key string) (interface{}, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	pv, is := l.Maps[key]
	if !is {
		if l.onMissed != nil {
			dt := l.onMissed(key)
			if dt == nil {
				return nil, false
			}
			if l.dList.Len() == l.capacity {
				l.replacePut(key, dt)
			} else {
				l.normalPut(key, dt)
			}
			return dt, true
		}
		return nil, false
	}
	l.dList.toHead(pv)
	return pv.Value, true
}

func (l *LruCache) Put(key string, value interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dList.Len() == l.capacity {
		l.replacePut(key, value)
	} else {
		l.normalPut(key, value)
	}
}

func (l *LruCache) normalPut(key string, value interface{}) {
	if n, ok := l.Maps[key]; ok {
		n.Value = value
		l.dList.toHead(n)
		return
	}
	vp := l.dList.PushFront(value)
	l.Maps[key] = vp
}
func (l *LruCache) replacePut(key string, value interface{}) {
	taiIter := l.dList.TailIter()
	for k, v := range l.Maps {
		if v == taiIter.P {
			if l.onEvited != nil {
				go l.onEvited(k, v.Value)
				break
			}
		}
	}
	taiIter.Delete()
	l.normalPut(key, value)

}
func (d *List) toHead(pv *LinkedListNode) {
	if pv == d.Head.Next {
		return
	}

	ptr := pv.Prev
	ptr.Next = pv.Next
	pv.Next.Prev = ptr

	sp := d.Head.Next
	pv.Next = sp
	pv.Prev = d.Head
	d.Head.Next = pv
	sp.Prev = pv
}
