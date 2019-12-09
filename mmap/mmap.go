package mmap

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"syscall"
)

type MMap struct {
	fd       int
	file     *os.File
	filename string
	data     []byte
}

func NewMMap(filename string, size int) *MMap {
	mm := new(MMap)
	mm.filename = filename
	mm.open()
	mm.resize(size)
	mm.mmap(size)
	return mm
}

func (mm *MMap) resize(size int) {
	log.Error("Resizing: ", size)
	err := syscall.Ftruncate(mm.fd, int64(size))
	if err != nil {
		log.Error("Error resizing: ", err)
	}
}

func (mm *MMap) open() {
	fmt.Println("Getting file descriptor")
	f, err := os.OpenFile(mm.filename, os.O_CREATE|os.O_RDWR, 0)
	if err != nil {
		fmt.Println("Could not open file: ", err)
	}
	mm.fd = int(f.Fd())
	mm.file = f
}

func (mm *MMap) mmap(size int) {
	log.Info("mmapping: ", size)
	data, err := syscall.Mmap(mm.fd, 0, size, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		log.Error("Error mmapping: ", err)
	}
	mm.data = data
}

func (mm *MMap) extend(size int) {
	mm.file.Close()
	mm.open()
	mm.resize(size)
	mm.mmap(size)
}

func (mm *MMap) GetData() []byte {
	return mm.data
}
