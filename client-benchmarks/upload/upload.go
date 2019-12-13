package main

import (
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
	var filenameFlag = flag.String("src", "parking-citations-50k.csv", "Source file")
	var addrFlag = flag.String("addr", "127.0.0.1:8100", "Server address")
	flag.Parse()

	filename := *filenameFlag

	data, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	//defer data.Close()
	fi, _ := data.Stat()
	fileSize := fi.Size()

	client := helpers.New(*addrFlag, 16, 16)
	writer, _ := client.OpenWriter(*dstFlag)

	startTime := time.Now()

	io.Copy(writer, data)
	data.Close()
	writer.Close()

	elapsed := time.Since(startTime)
	fmt.Printf("Tokk %v seconds\tSize: %v\tSpeed: %v", elapsed, fileSize, s3.ByteSpeed(fileSize, elapsed))

}
