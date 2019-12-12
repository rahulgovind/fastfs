package hybridcache

import (
	"github.com/rahulgovind/fastfs/cache"
	"github.com/rahulgovind/fastfs/cache/diskcache"
	"github.com/rahulgovind/fastfs/cache/memcache"
	log "github.com/sirupsen/logrus"
)

type HybridCache struct {
	mc *memcache.MemCache
	dc cache.Cache
}

func NewHybridCache(maxMemEntries int, dc cache.Cache) *HybridCache {
	hc := new(HybridCache)
	hc.mc = memcache.NewMemCache(maxMemEntries)
	hc.dc = dc
	hc.mc.OnEvicted = hc.handleMemEvict
	return hc
}

func NewMemDiskHybridCache(maxMemEntries int, maxDiskEntries int, blockSize int,
	filename string, iotype int) *HybridCache {
	hc := new(HybridCache)
	hc.mc = memcache.NewMemCache(maxMemEntries)
	hc.dc = diskcache.NewDiskCache(maxDiskEntries, blockSize, filename, iotype)
	hc.mc.OnEvicted = hc.handleMemEvict
	return hc

}

func (hc *HybridCache) handleMemEvict(key string, value []byte) {
	// Insert to disk
	hc.dc.Add(key, value)
}

// Add, Get, Remove, Len, Clear
func (hc *HybridCache) Add(key string, value []byte) {
	// Only add to memcache
	hc.mc.Add(key, value)
}

func (hc *HybridCache) Get(key string) ([]byte, bool) {
	// In memcache?
	if data, ok := hc.mc.Get(key); ok {
		log.Info("Memcache hit", key)
		return data, true
	}

	if data, ok := hc.dc.Get(key); ok {
		// Also insert in memcache
		log.Info("Diskcache hit", key)
		hc.mc.Add(key, data)
		return data, true
	}

	log.Info("No hit", key)

	return nil, false
}

// Approximate number of elements in the cache
func (hc *HybridCache) Len() int {
	return hc.mc.Len() + hc.dc.Len()
}

func (hc *HybridCache) Remove(key string) {
	hc.mc.Remove(key)
	hc.dc.Remove(key)
}

func (hc *HybridCache) Clear() {
	temp := hc.mc.OnEvicted
	hc.mc.OnEvicted = nil
	hc.mc.Clear()
	hc.mc.OnEvicted = temp

	hc.dc.Clear()
}
