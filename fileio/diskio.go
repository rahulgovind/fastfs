package fileio

type FileIO interface {
	ReadAt(offset int, size int) []byte
	WriteAt(offset int, b []byte)
}
