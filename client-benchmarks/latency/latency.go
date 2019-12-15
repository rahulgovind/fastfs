package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
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
	randomRange := flag.Int("random-range", 1024*1024*16, "Size of file or random range to query in")

	flag.Parse()
	url := *urlFlag
	if url == "" {
		log.Fatal("Please enter a valid URL")
	}

	start := time.Now()
	makeRequest(url, 0)
	elapsed := time.Since(start)
	fmt.Printf("%v to download 1K (Request 1)\n", elapsed)
	start = time.Now()
	makeRequest(url, 0)
	elapsed = time.Since(start)
	fmt.Printf("%v to download 1K (Request 2)", elapsed)

	// Random starting positions

	total := int64(0)
	numIterations := 10
	for i := 0; i < numIterations; i += 1 {
		offset := rand.Intn(*randomRange) - 1024
		if offset < 0 {
			offset = 0
		}
		start = time.Now()
		makeRequest(url, offset)
		elapsed = time.Since(start)
		fmt.Println(elapsed.Nanoseconds())
		total += elapsed.Nanoseconds()
	}

	fmt.Printf("Took average %v microseconds", float64(total)/float64(numIterations)/1000)
}
