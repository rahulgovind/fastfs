package hash

import (
	"encoding/binary"
	"hash/fnv"
)

func bigToLittle(x uint64) uint64 {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, x)
	return binary.LittleEndian.Uint64(b)
}

func bigToLittle32(x uint32) uint32 {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b, x)
	return binary.LittleEndian.Uint32(b)
}

func HashInt(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))

	r := int64((bigToLittle(h.Sum64()) >> 2) & 0x3fffffffffffffff)
	return r
}

func HashInt32(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)

	r := (bigToLittle32(h.Sum32()) >> 2) & 0x3fffffff
	return r
}
