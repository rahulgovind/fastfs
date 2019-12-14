package datamanager

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/groupcache/singleflight"
	"github.com/rahulgovind/fastfs/cache"
	"github.com/rahulgovind/fastfs/metadatamanager"
	"github.com/rahulgovind/fastfs/partitioner"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
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
	uploadChan     chan *UploadInput
	partitioner    partitioner.Partitioner
	bufChan        chan *bytes.Buffer
}

type DownloadElement struct {
	fLink string
	out   chan []byte
}

type UploadInput struct {
	buf   *bytes.Buffer
	path  string
	block int64
	sem   chan bool
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
	serverAddr string, mm *metadatamanager.MetadataManager, p partitioner.Partitioner) *DataManager {
	dm := new(DataManager)
	dm.cache = hc
	dm.bucket = bucket
	dm.numDownloaders = numDownloaders
	dm.downloader = s3.GetDownloader()
	dm.BlockSize = blockSize
	dm.requestCh = make(chan DownloadElement, 1024)
	dm.ServerAddr = serverAddr
	dm.mm = mm

	dm.uploadChan = make(chan *UploadInput, 128)
	dm.bufChan = make(chan *bytes.Buffer, 128)
	dm.partitioner = p

	for i := 0; i < 32; i += 1 {
		go dm.uploader()
	}

	for i := 0; i < 128; i += 1 {
		dm.bufChan <- bytes.NewBuffer(make([]byte, 0, dm.BlockSize))
	}


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
	r    io.ReadCloser
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

type CountingWriter struct {
	w    io.WriteCloser
	size int64
}

func NewCountingWriter(w io.WriteCloser) *CountingWriter {
	res := new(CountingWriter)
	res.w = w
	res.size = 0
	return res
}

func (cr *CountingWriter) Write(b []byte) (int, error) {
	n, err := cr.w.Write(b)
	cr.size += int64(n)
	return n, err
}

func (cr *CountingWriter) Close() error {
	return cr.w.Close()
}

func (cr *CountingWriter) Size() int64 {
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
			dir = path[:lastIndex+1]
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

func (dm *DataManager) uploader() {
	for {
		u := <-dm.uploadChan

		target := dm.partitioner.GetServer(u.path, u.block)

		url := fmt.Sprintf("http://%s/put/%s?block=%d", target, u.path, u.block)

		log.Println("Client put")

		maxRetries := 3
		numRetries := 0

		for {
			req, err := http.NewRequest("PUT", url, u.buf)
			log.Error("Starting PUT: %v", url)
			if err == io.EOF {
				break
			}

			if err != nil {
				numRetries += 1
				if numRetries <= maxRetries {
					time.Sleep(2 * time.Second)
					continue
				}
				log.Fatal(err)
			}

			client := &http.Client{}
			res, err := client.Do(req)
			if err != nil {
				log.Fatal(err)
			}

			res.Body.Close()
			log.Error("Ending PUT: %v", url)
			break
		}
		u.buf.Reset()
		dm.bufChan <- u.buf
		<- u.sem
	}
}
