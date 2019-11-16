package main

import (
	"bytes"
	"fmt"
	"github.com/golang/groupcache"
	"github.com/rahulgovind/fastfs/metadatamanager"
	"github.com/rahulgovind/fastfs/s3"
	"io/ioutil"
	"log"
	"time"
)


func main() {
	g := new(Getter)
	g.bucket = "speedfs"

	group := groupcache.NewGroup("fastfs", 1024*1024*1024, g)

	start := time.Now()
	for i := 0; i < 50; i += 1 {
		var data []byte
		err := group.Get(nil, "file2/2", groupcache.AllocatingByteSliceSink(&data))
		fmt.Println(i, "Len: ", len(data))
		if err != nil {
			log.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	fmt.Println(elapsed)
}
