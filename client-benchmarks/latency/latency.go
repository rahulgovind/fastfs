package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func makeRequest(url string, offset int) {
	var client http.Client
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Set range header
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+1023))
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func main() {
	urlFlag := flag.String("url", "", "URL to download from")
	startFlag := flag.Int("start", 0, "Offset to read from")

	flag.Parse()
	url := *urlFlag
	if url == "" {
		log.Fatal("Please enter a valid URL")
	}

	start := time.Now()
	makeRequest(url, *startFlag)
	elapsed := time.Since(start)
	fmt.Printf("%v to download 1K (Request 1)\n", elapsed)
	start = time.Now()
	makeRequest(url, *startFlag)
	elapsed = time.Since(start)
	fmt.Printf("%v to download 1K (Request 2)", elapsed)

	total := int64(0)
	numIterations := 10
	for i := 0; i < numIterations; i += 1 {
		start = time.Now()
		makeRequest(url, *startFlag)
		elapsed = time.Since(start)
		total += elapsed.Nanoseconds()
	}
	fmt.Printf("Took %v microseconds", float64(total)/float64(numIterations) / 1000)
}
