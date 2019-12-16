package main

import (
	"fmt"
	"github.com/rahulgovind/fastfs/s3"
	"os"
	"time"
)

func main() {
	f, _ := os.Open("parking-citations.csv")
	fi, _ := f.Stat()
	size := fi.Size()
	defer f.Close()
	start := time.Now()
	s3.PutOjbect("speedfs", "uploaded_file.xx.out", f)
	elapsed := time.Since(start)
	fmt.Printf("Size: %s\tTime taken: %v\tSpeed: %v\n", s3.ByteSize(size), elapsed, s3.ByteSpeed(size, elapsed))
}
