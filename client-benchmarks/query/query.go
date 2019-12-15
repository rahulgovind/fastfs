package main

import (
	"flag"
	"fmt"
	"github.com/rahulgovind/fastfs/helpers"
	"log"
	"os"
	"time"
)

// Measure latency using FastFS client suite
func main() {
	addrFlag := flag.String("addr", "localhost:8100", "Server address")
	fileFlag := flag.String("file", "parking-citations-500k.csv", "File to query ")
	conditinoFlag := flag.String("condition", "FERR", "Condition ")
	colFlag := flag.Int("column", 8, "Condition ")
	out := flag.String("out", "/dev/null", "Output file")
	flag.Parse()

	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	client := helpers.New(*addrFlag, 1, 1)
	// 3897 on entire dataset

	startTime := time.Now()
	client.Query(*fileFlag, 5, *conditinoFlag, *colFlag, f)
	elapsed := time.Since(startTime)
	fmt.Printf("Query took %v", elapsed)
}
