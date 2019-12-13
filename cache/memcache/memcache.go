package memcache

import (
	"container/list"
	"sync"
)

// MemCache is an LRU cache. It is not safe for concurrent access.
type MemCache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int64

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key string, value []byte)

	ll    *list.List
	cache map[interface{}]*list.Element
	mu    sync.RWMutex
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type entry struct {
	key   string
	value []byte
}

// NewMemCache creates a new MemCache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewMemCache(maxEntries int64) *MemCache {
	return &MemCache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Add adds a value to the cache.
func (mc *MemCache) Add(key string, value []byte) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.cache == nil {
		mc.cache = make(map[interface{}]*list.Element)
		mc.ll = list.New()
	}
	if ee, ok := mc.cache[key]; ok {
		mc.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return
	}
	ele := mc.ll.PushFront(&entry{key, value})
	mc.cache[key] = ele
	if mc.MaxEntries != 0 && int64(mc.ll.Len()) > mc.MaxEntries {
		mc.RemoveOldest()
	}
}

// Get looks up a key's value from the cache.
func (mc *MemCache) Get(key string) (value []byte, ok bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.cache == nil {
		return
	}
	if ele, hit := mc.cache[key]; hit {
		mc.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (mc *MemCache) Remove(key string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.cache == nil {
		return
	}
	if ele, hit := mc.cache[key]; hit {
		mc.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (mc *MemCache) RemoveOldest() {
	if mc.cache == nil {
		return
	}
	ele := mc.ll.Back()
	if ele != nil {
		mc.removeElement(ele)
	}
}

func (mc *MemCache) removeElement(e *list.Element) {
	mc.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(mc.cache, kv.key)
	if mc.OnEvicted != nil {
		mc.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (mc *MemCache) Len() int64 {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.cache == nil {
		return 0
	}
	return int64(mc.ll.Len())
}

// Clear purges all stored items from the cache.
func (mc *MemCache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.OnEvicted != nil {
		for _, e := range mc.cache {
			kv := e.Value.(*entry)
			mc.OnEvicted(kv.key, kv.value)
		}
	}
	mc.ll = nil
	mc.cache = nil
}
