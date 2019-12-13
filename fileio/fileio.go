package fileio

import (
	"io"
	"log"
	"os"
)

type DirectFileIO struct {
	file *os.File
}

func NewDirectFileIO(filename string, size int64) *DirectFileIO {
	dfio := new(DirectFileIO)

	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}

	file.Truncate(size)
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

	file, err = os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	dfio.file = file

	return dfio
}

func (dfio *DirectFileIO) ReadAt(offset int64, size int64) []byte {
	out := make([]byte, size)
	n, err := dfio.file.ReadAt(out, offset)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	out = out[:n]
	return out
}

func (dfio *DirectFileIO) WriteAt(offset int64, b []byte) {
	dfio.file.WriteAt(b, int64(offset))
}
