package main

import (
	"flag"
	"fmt"
	"github.com/rahulgovind/fastfs/helpers"
	"io"
	"io/ioutil"
	"time"
)

// Measure latency using FastFS client suite
func main() {
	addrFlag := flag.String("addr", "", "Server address")
	fileFlag := flag.String("file", "", "File to downloader (Only 1K)")
	flag.Parse()

	client := helpers.New(*addrFlag, 1, 1)
	reader := client.OpenOffsetReader(*fileFlag, 0)

	startTime := time.Now()
	io.CopyN(ioutil.Discard, reader, 1024)
	elapsed := time.Since(startTime)
	reader.Close()
	fmt.Printf("Downloading 1K of %v took %v", *fileFlag, elapsed)
}
