package cache

type ZItem struct {
	data   interface{}
	rCount int
	wCount int
}
type ZLruCache struct {
	lru      *LruCache
	onMissed MissCallback
	onEvited EvitedCallback
}

func NewZLruCache(cap int64) *ZLruCache {
	zl := &ZLruCache{}

	zl.lru = NewLruCache(cap, func(s string) interface{} {
		return zl.zget(s)
	}, func(s string, i interface{}) {
		zl.zsync(s, i)
	})

	return zl
}

func (zl *ZLruCache) BindMissed(call func(string) interface{}) {
	zl.onMissed = call
}
func (zl *ZLruCache) BindEvited(call func(string, interface{})) {
	zl.onEvited = call
}
func (zl *ZLruCache) RGet(s string) (interface{}, bool) {

	in, ok := zl.lru.Get(s)
	if !ok {
		return nil, false
	}

	item := in.(*ZItem)
	item.rCount++
	return item.data, true
}

func (zl *ZLruCache) WGet(s string) (interface{}, bool) {
	in, ok := zl.lru.Get(s)
	if !ok {
		return nil, false
	}

	item := in.(*ZItem)
	item.wCount++
	return item.data, true
}
// new to cache,some time fresh to the 
func (zl *ZLruCache) Put(s string, i interface{}) {
	zl.lru.Put(s, &ZItem{
		data:   i,
		rCount: 0,
		wCount: 1,
	})
}
func (zl *ZLruCache) Len() int {
	return zl.lru.Len()
}

func (zl *ZLruCache) Clear() {
	zl.lru.Clear()
}

func (zl *ZLruCache) zget(s string) interface{} {
	if zl.onMissed != nil {
		i := zl.onMissed(s)
		if i == nil {
			return nil
		}

		return &ZItem{
			data:   i,
			rCount: 0,
			wCount: 0,
		}
	}
	return nil
}

func (zl *ZLruCache) zsync(s string, z interface{}) {
	item := z.(*ZItem)

	if item.wCount == 0 {
		return
	}

	if zl.onEvited != nil {
		zl.onEvited(s, item.data)
	}
}
