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
	dm.gc = groupcache.NewGroup(bucket, 512*1024*1024*1024, dm)
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

func (dm *DataManager) downloadHandler(path string, w io.Writer) {
	var data []byte
	for i := 0; ; i += 1 {
		go dm.gc.Get(nil, CacheKeyToString(path, i + 1), groupcache.AllocatingByteSliceSink(&data))
		err := dm.gc.Get(nil, CacheKeyToString(path, i), groupcache.AllocatingByteSliceSink(&data))
		w.Write(data)
		if err != nil {
			log.Fatal(err)
		}
		if len(data) < int(metadatamanager.BlockSize) {
			break
		}
	}
}

func (dm *DataManager) LoadServer(addr string, port int) {
	s := NewServer(addr, port, dm.downloadHandler)
	s.Serve()
}
