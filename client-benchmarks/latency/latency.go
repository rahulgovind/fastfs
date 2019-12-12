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

func makeRequest(url string) {
	var client http.Client
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Set range header
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", 0, 1023))
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func main() {
	urlFlag := flag.String("url", "", "URL to download from")

	flag.Parse()
	url := *urlFlag
	if url == "" {
		log.Fatal("Please enter a valid URL")
	}

	makeRequest(url)
	start := time.Now()
	makeRequest(url)
	elapsed := time.Since(start)
	fmt.Printf("Took %v to download 1K", elapsed)
}
