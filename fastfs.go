package main

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	log "github.com/sirupsen/logrus"
	"sync"
)

//import (
//	"fmt"
//	"github.com/golang/groupcache"
//	"log"
//	"time"
//)
//
//func main() {
//	g := new(Getter)
//	g.bucket = "speedfs"
//
//	group := groupcache.NewGroup("fastfs", 1024*1024*1024, g)
//
//	start := time.Now()
//	for i := 0; i < 50; i += 1 {
//		var data []byte
//		err := group.Get(nil, "file2/2", groupcache.AllocatingByteSliceSink(&data))
//		fmt.Println(i, "Len: ", len(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//	elapsed := time.Since(start)
//	fmt.Println(elapsed)
//}

type FastFS struct {
	mu          sync.RWMutex
	mlistConfig *memberlist.Config
	servers     []string
	Event       memberlist.EventDelegate
	mlist       *memberlist.Memberlist
}

func NewFastFS(port int, events memberlist.EventDelegate) *FastFS {
	ffs := new(FastFS)

	config := memberlist.DefaultLocalConfig()

	config.BindPort = port
	config.AdvertisePort = port
	config.Name = fmt.Sprintf("localhost:%v", 8081)
	config.Events = ffs
	ffs.Event = events

	ffs.mlistConfig = config

	list, err := memberlist.Create(config)
	if err != nil {
		log.Fatal(err)
	}

	ffs.mlist = list

	return ffs
}

func (ffs *FastFS) NotifyJoin(n *memberlist.Node) {
	if ffs.Event != nil {
		ffs.Event.NotifyJoin(n)
	}
}

func (ffs *FastFS) NotifyLeave(n *memberlist.Node) {
	if ffs.Event != nil {
		ffs.Event.NotifyJoin(n)
	}
}

func (ffs *FastFS) NotifyUpdate(n *memberlist.Node) {
	if ffs.Event != nil {
		ffs.Event.NotifyJoin(n)
	}
}

func (ffs *FastFS) GetServers() []string {
	var res []string
	for _, node := range ffs.mlist.Members() {
		res = append(res, node.Name)
	}
	return res
}
