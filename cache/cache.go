package cache

type Cache interface {
	Add(key string, value []byte)
	Get(key string) ([]byte, bool)
	Remove(key string)
	Len() int64
	Clear()
}
