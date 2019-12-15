package diskcache

import (
	"container/list"
	"github.com/rahulgovind/fastfs/fileio"
	log "github.com/sirupsen/logrus"
	"sync"
)

// DiskCache is an LRU cache. It is not safe for concurrent access.
type DiskCache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int64

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key Key)

	ll        *list.List
	cache     map[interface{}]*list.Element
	blockSize int64
	bm        *fileio.BlockManager

	mu sync.RWMutex
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type Key interface{}

type entry struct {
	key     string
	blockId int64
	length  int64
}

// NewDiskCache creates a new DiskCache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewDiskCache(maxEntries int64, blockSize int64, filename string, iotype int) *DiskCache {
	return &DiskCache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
		bm:         fileio.NewBlockManager(filename, maxEntries+100, blockSize, iotype),
	}
}

// Add adds a value to the cache.
func (c *DiskCache) Add(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}

	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		//c.removeElement(ee)
		return
	}

	blockId, _ := c.bm.Put(value)
	ele := c.ll.PushFront(&entry{key, blockId, int64(len(value))})

	c.cache[key] = ele
	if c.MaxEntries != 0 && int64(c.ll.Len()) > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *DiskCache) Get(key string) (value []byte, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cache == nil {
		return
	}

	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		blockId := ele.Value.(*entry).blockId
		data, _ := c.bm.Get(blockId, ele.Value.(*entry).length)
		return data, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *DiskCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *DiskCache) RemoveOldest() {
	if c.cache == nil {
		return
	}

	log.Info("Evicting from disk")
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *DiskCache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	//logrus.Debug("Freeing block ", kv.blockId)
	c.bm.Free(kv.blockId)

	if c.OnEvicted != nil {
		c.OnEvicted(kv.key)
	}
}

// Len returns the number of items in the cache.
func (c *DiskCache) Len() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.cache == nil {
		return 0
	}
	return int64(c.ll.Len())
}

// Clear purges all stored items from the cache.
func (c *DiskCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.bm.Free(kv.blockId)
			c.OnEvicted(kv.key)
		}
	}
	c.ll = nil
	c.cache = nil
}
