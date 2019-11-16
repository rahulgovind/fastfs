package datamanager

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/golang/groupcache"
	"github.com/rahulgovind/fastfs/lru"
	"github.com/rahulgovind/fastfs/metadatamanager"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
)

func CacheKeyToString(path string, block int) string {
	return fmt.Sprintf("%v-%v", path, block)
}

func StringToCacheKey(s string) (string, int) {
	idx := strings.LastIndex(s, "-")
	block, err := strconv.ParseInt(s[idx+1:], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return s[:idx], int(block)
}

type DataManager struct {
	DownloadQueue  *queue.Queue
	cache          *lru.Cache
	bucket         string
	numDownloaders int
	gc             *groupcache.Group
	downloader     *s3manager.Downloader
}

func (dm *DataManager) Downloader() {
	for {
		elem, err := dm.DownloadQueue.Get(1)
		if err != nil {
			log.Fatal(err)
		}
		v := elem[0].(DownloadElement)

		buf := bytes.NewBuffer(make([]byte, 0, metadatamanager.BlockSize))
		s3.DownloadToWriter(dm.bucket, v.fLink, buf)
		dm.cache.Add(v.fLink, buf)
		v.cond.Signal()
	}
}

func New(bucket string, numDownloaders int) *DataManager {
	dm := new(DataManager)
	dm.cache = lru.New(1024)
	dm.bucket = bucket
	dm.DownloadQueue = queue.New(1024)
	dm.numDownloaders = numDownloaders
	dm.gc = groupcache.NewGroup(bucket, 256*1024*1024, dm)
	dm.downloader = s3.GetDownloader()
	return dm
}

func (dm *DataManager) Start() {
	for i := 0; i < dm.numDownloaders; i += 1 {
		go dm.Downloader()
	}
}

func (dm *DataManager) DirectDownload(path string, w io.Writer) {
	err := s3.DownloadToWriter(dm.bucket, path, w)
	if err != nil {
		log.Fatal(err)
	}
}

type DownloadElement struct {
	fLink string
	cond  *sync.Cond
}

func (dm *DataManager) Get(ctx groupcache.Context, key string, dest groupcache.Sink) error {
	log.Infof("Get %v", key)

	buf := bytes.NewBuffer(make([]byte, 0, metadatamanager.BlockSize))
	path, block := StringToCacheKey(key)

	err := s3.DownloadToWriterPartial(dm.bucket, dm.downloader, path, buf,
		int64(block)*metadatamanager.BlockSize,
		metadatamanager.BlockSize)

	if err != nil {
		log.Error(err)
		return err
	}

	b, _ := ioutil.ReadAll(buf)
	dest.SetBytes(b)
	return nil
}

func (dm *DataManager) downloaderFunc(path string, d chan int, out chan *BlockData) {
	for {
		block := <-d
		if block == -1 {
			break
		}
		//log.Infof("Processing %v", block)
		var data []byte
		err := dm.gc.Get(nil, CacheKeyToString(path, block), groupcache.AllocatingByteSliceSink(&data))

		if err != nil {
			log.Fatal(err)
		}
		out <- &BlockData{data, block}
	}
}

type BlockData struct {
	data    []byte
	version int
}

func (dm *DataManager) downloadHandler(path string, w io.Writer) {
	nextBlock := 0
	nextDownload := 0
	stop := false
	blocks := make(map[int]*BlockData)

	d := make(chan int, 4)
	out := make(chan *BlockData)

	for i := 0; i < 8; i++ {
		d <- nextDownload
		nextDownload += 1
		//log.Infof("Added %v", nextDownload - 1)
		go dm.downloaderFunc(path, d, out)
	}

	for !stop {
		bd := <-out
		//log.Infof("Got block %v", bd.version)
		if len(bd.data) < int(metadatamanager.BlockSize) {
			blocks[bd.version] = bd
			stop = true
			break
		}

		blocks[bd.version] = bd

		for {
			bdNext, ok := blocks[nextBlock]
			if !ok {
				break
			}
			d <- nextDownload
			//log.Infof("Added %v", nextDownload )
			nextDownload += 1
			w.Write(bdNext.data)
			delete(blocks, nextBlock)
			nextBlock += 1
		}
	}

	//log.Infof("Done adding blocks. Last added %v", nextDownload)

	for nextBlock < nextDownload {
		//log.Infof("%v %v", nextBlock, nextDownload)
		bdNext, ok := blocks[nextBlock]
		if !ok {
			//log.Infof("Waiting for block %v", nextBlock)
			bd := <-out
			//log.Infof("Got block %v", bd.version)
			blocks[bd.version] = bd
			continue
		}
		w.Write(bdNext.data)
		//log.Infof("Deleting block %v", nextBlock)
		delete(blocks, nextBlock)
		nextBlock += 1
	}

	for i := 0; i < 8; i+= 1{
		d <- -1
	}
}

func (dm *DataManager) LoadServer(addr string, port int) {
	s := NewServer(addr, port, dm.downloadHandler)
	s.Serve()
}
