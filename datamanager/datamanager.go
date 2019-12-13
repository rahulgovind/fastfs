package datamanager

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/groupcache/singleflight"
	"github.com/rahulgovind/fastfs/cache"
	"github.com/rahulgovind/fastfs/metadatamanager"
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
	ServerAddr     string
	mm             *metadatamanager.MetadataManager
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

func New(bucket string, numDownloaders int, hc cache.Cache, blockSize int64,
	serverAddr string, mm *metadatamanager.MetadataManager) *DataManager {
	dm := new(DataManager)
	dm.cache = hc
	dm.bucket = bucket
	dm.numDownloaders = numDownloaders
	dm.downloader = s3.GetDownloader()
	dm.BlockSize = blockSize
	dm.requestCh = make(chan DownloadElement, 1024)
	dm.ServerAddr = serverAddr
	dm.mm = mm
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

func (dm *DataManager) CacheGet(path string, block int64) ([]byte, bool) {
	fLink := CacheKeyToString(path, block)
	return dm.cache.Get(fLink)
}

func (dm *DataManager) CacheDelete(path string, block int64) {
	fLink := CacheKeyToString(path, block)
	dm.cache.Remove(fLink)
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

		if dm.mm != nil {
			dm.mm.SetLocation(path, block, dm.ServerAddr)
		}
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

func (dm *DataManager) CachePut(path string, block int64, data []byte) error {
	fLink := CacheKeyToString(path, block)
	fmt.Println("Adding to cache: ", fLink)

	if dm.mm != nil {
		dm.mm.SetLocation(path, block, dm.ServerAddr)
	}
	dm.cache.Add(fLink, data)

	return nil
}

type CountingReader struct {
	r io.ReadCloser
	size int64
}

func (cr *CountingReader) Read(b []byte) (int, error) {
	n, err := cr.r.Read(b)
	cr.size += int64(n)
	return n, err
}

func (cr *CountingReader) Close() error {
	return cr.r.Close()
}

func (cr *CountingReader) Size() int64 {
	return cr.size
}

func (dm *DataManager) Upload(path string, r io.ReadCloser) {
	cr := &CountingReader{r, 0}
	err := s3.PutOjbect(dm.bucket, path, cr)
	if err != nil {
		log.Fatal(err)
	}
	if dm.mm != nil {
		lastIndex := strings.LastIndex(path, "/")
		dir := ""
		if lastIndex != -1 {
			dir = path[:lastIndex + 1]
		}
		dm.mm.AddToList(dir, path, cr.Size())
		log.Infof("Adding to metadata: Dir:%s\tFile:%s\tSize: %d", dir, path, cr.Size())
	}
}

func (dm *DataManager) Delete(path string) {
	err := s3.DeleteObject(dm.bucket, path)
	if err != nil {
		log.Error(err)
	}
}
