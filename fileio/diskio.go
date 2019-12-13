package fileio

type FileIO interface {
	ReadAt(offset int64, size int64) []byte
	WriteAt(offset int64, b []byte)
}
