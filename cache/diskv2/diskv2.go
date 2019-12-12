package diskv2

import "github.com/peterbourgon/diskv"

type DiskV2Cache struct {
	dv *diskv.Diskv
}

func NewDiskV2Cache(dir string, maxEntries int) *DiskV2Cache {
	flatTransform := func(s string) []string { return []string{} }
	d := new(DiskV2Cache)
	d.dv = diskv.New(diskv.Options{
		BasePath:     dir,
		Transform:    flatTransform,
		CacheSizeMax: uint64(maxEntries),
	})
	return d
}

func (d *DiskV2Cache) Add(key string, value []byte) {
	d.dv.Write(key, value)
}

func (d *DiskV2Cache) Get(key string) ([]byte, bool) {
	if d.dv.Has(key) {
		value, _ := d.dv.Read(key)
		return value, true
	}
	return nil, false
}

func (d *DiskV2Cache) Clear() {
	d.dv.EraseAll()
}

func (d *DiskV2Cache) Len() int {
	return -1
}

func (d *DiskV2Cache) Remove(key string) {
	d.dv.Erase(key)
}
