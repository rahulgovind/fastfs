package main

import (
	"flag"
	"fmt"
	"github.com/rahulgovind/fastfs/datamanager"
	"github.com/rahulgovind/fastfs/helpers"
	"github.com/rahulgovind/fastfs/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func main() {
	serverAddr := flag.String("server-addr", "127.0.0.1:8100", "Server to download from")
	file := flag.String("filename", "", "File to download")
	out := flag.String("out", "", "Output location. Output discarded if empty string passed.")
	flag.Parse()

	client := helpers.New(*serverAddr, 8, 8)
	var w io.WriteCloser = &datamanager.FakeWriteCloser{Writer: ioutil.Discard}
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			log.Fatal(err)
		}
		w = f
	}
	w.Close()

	log.Println("Starting write")

	//w2 := datamanager.NewCountingWriter(w)
	r := client.OpenOffsetReader(*file, 0)
	start := time.Now()
	client.WriteTo(w, *file, 0, -1)
	written, _ := io.Copy(ioutil.Discard, r)
	elapsed := time.Since(start)

	//written := w2.Size()
	fmt.Printf("Size: %s\tTime taken: %v\tSpeed: %v\n", s3.ByteSize(written), elapsed, s3.ByteSpeed(written, elapsed))
	//fmt.Println(n, string(b))
}
