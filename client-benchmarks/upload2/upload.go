package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/rahulgovind/fastfs/helpers"
	"github.com/rahulgovind/fastfs/s3"
	"io"
	"log"
	"os"
	"time"
)

func main() {
	var dstFlag = flag.String("dst", "uploaded_file4.out", "Desination filename")
	var filenameFlag = flag.String("src", "parking-citations-500k.csv", "Source file")
	var addrFlag = flag.String("addr", "127.0.0.1:8100", "Server address")
	var smallFlag = flag.Bool("small", false, "Test on 1K random data")

	flag.Parse()

	filename := *filenameFlag

	var data io.Reader
	var fileSize int64
	if !*smallFlag {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()

		fi, _ := file.Stat()
		fileSize = fi.Size()

		data = file
	} else {
		fileSize = 1024
		data = bytes.NewBuffer(make([]byte, 1024))
	}

	client := helpers.New(*addrFlag, 16, 16)
	writer, _ := client.BlockUploadWriter(*dstFlag)

	startTime := time.Now()
	io.Copy(writer, data)
	writer.Close()

	elapsed := time.Since(startTime)
	fmt.Printf("Tokk %v seconds\tSize: %v\tSpeed: %v", elapsed, fileSize, s3.ByteSpeed(fileSize, elapsed))
}
