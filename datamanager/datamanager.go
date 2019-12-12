package datamanager

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/groupcache/singleflight"
	"github.com/rahulgovind/fastfs/cache"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
	"strings"
)

func CacheKeyToString(path string, block int64) string {
	return fmt.Sprintf("%v-%v", path, block)
}

func StringToCacheKey(s string) (string, int64) {
	idx := strings.LastIndex(s, "-")
	block, err := strconv.ParseInt(s[idx+1:], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return s[:idx], block
}

type DataManager struct {
	cache          cache.Cache
	bucket         string
	numDownloaders int
	downloader     *s3manager.Downloader
	requestCh      chan DownloadElement
	g              singleflight.Group
	BlockSize      int64
}

type DownloadElement struct {
	fLink string
	out   chan []byte
}

type BlockGetter interface {
	Get(string, int64) ([]byte, error)
	GetBlockSize() int64
}

type BlockPutter interface {
	Put(string, int64, []byte) error
	GetBlockSize() int64
}

func New(bucket string, numDownloaders int, hc cache.Cache, blockSize int64) *DataManager {
	dm := new(DataManager)
	dm.cache = hc
	dm.bucket = bucket
	dm.numDownloaders = numDownloaders
	dm.downloader = s3.GetDownloader()
	dm.BlockSize = blockSize
	dm.requestCh = make(chan DownloadElement, 1024)
	dm.Start()
	return dm
}

func (dm *DataManager) Start() {
	for i := 0; i < dm.numDownloaders; i += 1 {
		go dm.downloadWorker()
	}
}

// Given path and block number download file
func (dm *DataManager) download(path string, block int64) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, dm.BlockSize))

	err := s3.DownloadToWriterPartial(dm.bucket, dm.downloader, path, buf,
		block*dm.BlockSize,
		dm.BlockSize,
	)

	if err != nil {
		log.Error(err)
		return nil, err
	}

	return buf.Bytes(), nil
}

func (dm *DataManager) downloadWorker() {
	for {
		log.Debug("Starting worker")
		req := <-dm.requestCh

		fLink := req.fLink
		path, block := StringToCacheKey(fLink)
		if block == -1 {
			break
		}

		var err error = nil

		data, err := dm.download(path, block)

		if err != nil {
			log.Error(err)
		}

		req.out <- data
	}
}

func (dm *DataManager) uniqueGet(path string, block int64) ([]byte, error) {
	log.Debugf("uniqueGet: %v %v", path, block)

	// In Cache?
	fLink := CacheKeyToString(path, block)
	data, ok := dm.cache.Get(fLink)
	if !ok {
		// Need to download :(
		ch := make(chan []byte, 1)
		dm.requestCh <- DownloadElement{fLink, ch}
		data = <-ch
		dm.cache.Add(fLink, data)
	}
	return data, nil
}

// Get deduplicates identical requests
func (dm *DataManager) Get(path string, block int64) ([]byte, error) {
	log.Debugf("Get: %v %v", path, block)
	fLink := CacheKeyToString(path, block)
	data, err := dm.g.Do(fLink, func() (data interface{}, err error) {
		data, err = dm.uniqueGet(path, block)
		return
	})

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	log.Debugf("Get done: %v %v %v", path, block, len(data.([]byte)))
	return data.([]byte), nil
}

func (dm *DataManager) GetBlockSize() int64 {
	return dm.BlockSize
}

func (dm *DataManager) Put(path string, block int64, data []byte) error {
	fLink := CacheKeyToString(path, block)
	fmt.Println("Adding to cache: ", fLink)

	dm.cache.Add(fLink, data)
	return nil
}

func (dm *DataManager) Upload(path string, r io.Reader) {
	err := s3.PutOjbect(dm.bucket, path, r)
	if err != nil {
		log.Error(err)
	}
}
