package main

import (
	"flag"
	"fmt"
	"github.com/rahulgovind/fastfs/s3"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	var dstFlag = flag.String("dst", "uploaded_file.out", "Desination filename")
	var filenameFlag = flag.String("src", "parking-citations-500k.csv", "Source file")

	flag.Parse()
	dst := *dstFlag
	filename := *filenameFlag

	data, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	fi, _ := data.Stat()
	fileSize := fi.Size()

	defer data.Close()

	startTime := time.Now()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:8100/put/%v", dst), data)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(startTime)
	fmt.Printf("Tokk %v seconds\tSize: %v\tSpeed: %v", elapsed, fileSize, s3.ByteSpeed(fileSize, elapsed))
	defer res.Body.Close()
}
