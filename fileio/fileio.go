package fileio

import (
	"log"
	"os"
)

type DirectFileIO struct {
	file *os.File
}

func NewDirectFileIO(filename string, size int) *DirectFileIO {
	dfio := new(DirectFileIO)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	dfio.file = file
	return dfio
}

func (dfio *DirectFileIO) ReadAt(offset int, size int) []byte {
	out := make([]byte, size)
	n, err := dfio.file.ReadAt(out, int64(offset))
	if err != nil {
		log.Fatal(err)
	}
	out = out[:n]
	return out
}

func (dfio *DirectFileIO) WriteAt(offset int, b []byte) {
	dfio.file.WriteAt(b, int64(offset))
}
