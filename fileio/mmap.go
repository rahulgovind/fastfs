package fileio

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

type MMapIO struct {
	data []byte
	mm   *MMap
}

func NewMMap(filename string, size int64) *MMap {
	mm := new(MMap)
	mm.filename = filename
	mm.open()
	mm.resize(size)
	mm.mmap(size)
	return mm
}

func (mm *MMap) resize(size int64) {
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

func (mm *MMap) mmap(size int64) {
	log.Info("mmapping: ", size, int64(size))
	data, err := syscall.Mmap(mm.fd, 0, int(size), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		log.Error("Error mmapping: ", err)
	}
	mm.data = data
}

func (mm *MMap) extend(size int64) {
	mm.file.Close()
	mm.open()
	mm.resize(size)
	mm.mmap(size)
}

func NewMMapIO(filename string, size int64) *MMapIO {
	mmio := new(MMapIO)
	mmio.mm = NewMMap(filename, size)
	mmio.data = mmio.mm.GetData()
	return mmio
}

func (mm *MMap) GetData() []byte {
	return mm.data
}

func (mmio *MMapIO) ReadAt(offset int64, size int64) []byte {
	return mmio.data[offset : offset+size]
}

func (mmio *MMapIO) WriteAt(offset int64, b []byte) {
	copy(mmio.data[offset:int(offset)+len(b)], b)
}
