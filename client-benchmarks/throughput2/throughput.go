package main

import (
	"flag"
	"github.com/rahulgovind/fastfs/datamanager"
	"github.com/rahulgovind/fastfs/helpers"
	"io"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	serverAddr := flag.String("server-addr", "127.0.0.1:8100", "Server to download from")
	file := flag.String("filename", "", "File to download")
	out := flag.String("out", "", "Output location. Output discarded if empty string passed.")
	flag.Parse()

	client := helpers.New(*serverAddr, 16, 8)
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
	r := client.OpenOffsetReader(*file, 0)
	b := make([]byte, 15)
	io.ReadAtLeast(r, b, len(b))
	//fmt.Println(n, string(b))
}
