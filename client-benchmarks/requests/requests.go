package main

import (
	"flag"
	"fmt"
	"github.com/rahulgovind/fastfs/helpers"
	"time"
)

func main() {
	serverAddr := flag.String("server-addr", "127.0.0.1:8100", "Server to download from")
	flag.Parse()

	client := helpers.New(*serverAddr, 16, 32)
	start := time.Now()
	fi, _ := client.Stat("nameFile2")
	elapsed := time.Since(start)

	fmt.Println(fi, elapsed)
}
