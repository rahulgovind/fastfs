package helpers

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/golang-lru"
	"github.com/rahulgovind/fastfs/common"
	"github.com/rahulgovind/fastfs/consistenthash"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	primaryAddr  string
	servers      []string
	BlockSize    int64
	LookAhead    int
	Queue        chan *InputData
	cmap         *consistenthash.Map
	objectCache  *lru.Cache
	S3UploadChan chan *BlockUploadInput
}

type InputData struct {
	path  string
	block int64
	buf   *bytes.Buffer
	out   chan *BlockData
}

type BlockData struct {
	data  *bytes.Buffer
	path  string
	block int64
}

type BlockUploadInput struct {
	filepath string
	block    int64
	data     []byte
	sem      chan bool
	wg       *sync.WaitGroup
}

type DownloadReadCloser struct {
	w io.WriteCloser
	r io.Reader
}

func NewDownloadReadCloser(c *Client, path string, start int64, end int64) *DownloadReadCloser {
	d := new(DownloadReadCloser)
	reader, writer := io.Pipe()
	d.r = reader
	d.w = writer

	go c.WriteTo(writer, path, start, end)
	return d
}

func (d *DownloadReadCloser) Read(b []byte) (int, error) {
	return d.r.Read(b)
}

func (d *DownloadReadCloser) Close() error {
	return d.w.Close()
}

type UploadWriteCloser struct {
	respBody       io.ReadCloser
	writer         io.WriteCloser
	bufferedWriter *bufio.Writer
	reader         io.ReadCloser
	wg             sync.WaitGroup
	c              *Client
}

func NewUploadWriteCloser(c *Client, path string) *UploadWriteCloser {
	u := new(UploadWriteCloser)
	u.c = c

	u.reader, u.writer = io.Pipe()
	u.bufferedWriter = bufio.NewWriterSize(u.writer, 1024*1024)

	u.wg = sync.WaitGroup{}
	u.wg.Add(1)
	go func() {
		c.ReadFrom(u.reader, path)
		u.wg.Done()
	}()
	return u
}

func (u *UploadWriteCloser) Write(b []byte) (int, error) {
	return u.bufferedWriter.Write(b)
}

func (u *UploadWriteCloser) Close() error {
	u.bufferedWriter.Flush()
	u.writer.Close()
	u.wg.Wait()
	return nil
}

func makeRequest(url string, method string) (*http.Response, error) {
	var client http.Client
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
		log.Fatal(err)
	}

	return resp, nil
}

func makeRangeRequest(url string, method string, start int64, end int64) (*http.Response, error) {
	var client http.Client
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
		log.Fatal(err)
	}

	return resp, nil
}

func New(addr string, numDownloaders int, lookAhead int) *Client {
	c := new(Client)
	c.primaryAddr = addr
	c.LookAhead = lookAhead
	c.Queue = make(chan *InputData, 10000)

	c.cmap = consistenthash.New(7, nil)
	c.objectCache, _ = lru.New(10240)
	c.S3UploadChan = make(chan *BlockUploadInput, 32)

	c.getServers()

	for i := 0; i < lookAhead; i += 1 {
		go c.downloader()
	}

	for i := 0; i < lookAhead; i += 1 {
		go c.blockUploader()
	}
	return c
}

type ServerResponse struct {
	Servers   []string
	BlockSize int64
}

func (c *Client) downloader() {
	for {
		input := <-c.Queue

		//log.Info("Downloading ", input.path, input.block)
		c.getBlock(input.path, input.block, input.buf)
		input.out <- &BlockData{input.buf, input.path, input.block}
	}
}

func (c *Client) getServers() {
	var client http.Client
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/setup", c.primaryAddr), nil)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	io.Copy(buf, resp.Body)

	var result ServerResponse
	err = json.Unmarshal(buf.Bytes(), &result)
	if err != nil {
		log.Fatal(err)
	}
	c.servers = result.Servers
	c.BlockSize = result.BlockSize
	c.cmap.Add(c.servers...)
}

func (c *Client) getBlock(path string, block int64, w io.Writer) error {
	target := c.cmap.Get(fmt.Sprintf("%s:%d", path, block))

	var client http.Client
	req, err := http.NewRequest("GET",
		fmt.Sprintf("http://%s/data/%s?block=%d", target, path, block), nil)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)

	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func (c *Client) ReadFrom(r io.ReadCloser, path string) {
	var client = &http.Client{
		//CheckRedirect: func(req *http.Request, via []*http.Request) error {
		//	return http.ErrUseLastResponse
		//},
	}

	url := fmt.Sprintf("http://%s/put/%s", c.servers[rand.Intn(len(c.servers))], path)
	req, err := http.NewRequest("PUT", url, r)

	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)

	if err != nil {
		log.Fatal(err)
	}

	resp.Body.Close()
}

type OffsetReader struct {
	offset    int64
	path      string
	client    *Client
	reader    io.ReadCloser
	writer    io.WriteCloser
	size      int64
	lookAhead int
	endBlock  int64
}

func NewOffsetReader(client *Client, path string, offset int64) *OffsetReader {
	r := new(OffsetReader)
	r.path = path
	r.offset = offset
	r.client = client
	r.reader, r.writer = io.Pipe()

	fi, _ := r.client.Stat(path)

	blockSize := r.client.BlockSize
	maxBlocks := (fi.Size - offset + blockSize - 1) / blockSize
	if offset%blockSize == 0 && fi.Size%blockSize == 0 {
		maxBlocks -= 1
	}

	r.lookAhead = client.LookAhead
	if r.lookAhead > int(maxBlocks) {
		r.lookAhead = int(maxBlocks)
	}

	r.endBlock = (fi.Size + blockSize - 1) / blockSize

	// Load reads stuff written by the downloaders one block at a time
	go r.load()

	return r
}

func (r *OffsetReader) load() {
	blockSize := r.client.BlockSize
	startBlock := r.offset / r.client.BlockSize
	startOff := r.offset % blockSize

	nextBlock := startBlock
	nextDownload := startBlock

	blocks := make(map[int64]*BlockData)

	out := make(chan *BlockData, r.lookAhead)

	//log.Infof("Ranges: %v\t%v", start, end)
	readBuffer := bytes.NewBuffer(make([]byte, 0, blockSize))

	for i := 0; i < r.lookAhead; i++ {
		r.client.Queue <- &InputData{r.path, nextDownload,
			bytes.NewBuffer(make([]byte, 0, blockSize)), out}
		nextDownload += 1
	}

	size := int64(0)

	toRead := nextDownload <= r.endBlock
	toAdd := true

	for nextBlock < nextDownload {
		bd := <-out

		// Should I add more?
		if len(bd.data.Bytes()) < int(blockSize) {
			toAdd = false
		}

		blocks[bd.block] = bd

		for nextBlock < nextDownload {
			bdNext, ok := blocks[nextBlock]
			if !ok {
				break
			}

			if toRead {
				readBuffer.Reset()
				readBuffer, bdNext.data = bdNext.data, readBuffer

				startOffHere := int64(0)
				endOffHere := int64(len(readBuffer.Bytes()))

				if bdNext.block == startBlock {
					startOffHere = startOff
				}

				if endOffHere-startOffHere > 0 {
					size += endOffHere - startOffHere

					if startOffHere != 0 {
						io.CopyN(ioutil.Discard, readBuffer, startOffHere)
					}

					_, err := io.Copy(r.writer, readBuffer)
					if err != nil {
						log.Error(err)
						toRead = false
						toAdd = false
					}
				}
			}

			if toAdd {
				r.client.Queue <- &InputData{r.path, nextDownload,
					bytes.NewBuffer(make([]byte, 0, blockSize)), out}
				nextDownload += 1
				toAdd = nextDownload <= r.endBlock
			}

			nextBlock += 1
		}
	}
	r.writer.Close()
}

func (r *OffsetReader) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *OffsetReader) Close() error {
	return r.writer.Close()
}

func (c *Client) OpenOffsetReader(path string, startAt int64) io.ReadCloser {
	return NewOffsetReader(c, path, startAt)
}

func (c *Client) WriteTo(w io.WriteCloser, path string, start int64, end int64) {
	startTime := time.Now()
	blockSize := c.BlockSize
	if end == -1 {
		end = math.MaxInt32
	}

	startBlock := start / c.BlockSize
	endBlock := end / blockSize

	startOff := start % blockSize
	endOff := end%blockSize + 1

	nextBlock := startBlock
	nextDownload := startBlock
	stop := false
	blocks := make(map[int64]*BlockData)
	out := make(chan *BlockData, c.LookAhead)

	//log.Infof("Ranges: %v\t%v", start, end)
	readBuffer := bytes.NewBuffer(make([]byte, 0, c.BlockSize))

	for i := 0; i < c.LookAhead; i++ {

		c.Queue <- &InputData{path, nextDownload, bytes.NewBuffer(make([]byte, 0, c.BlockSize)), out}
		nextDownload += 1
	}

	size := int64(0)

	for !stop {
		if nextDownload > endBlock {
			break
		}

		bd := <-out

		if len(bd.data.Bytes()) < int(c.BlockSize) {
			blocks[bd.block] = bd
			stop = true
			break
		}

		blocks[bd.block] = bd
		for {
			bdNext, ok := blocks[nextBlock]
			if !ok {
				break
			}

			readBuffer.Reset()
			readBuffer, bdNext.data = bdNext.data, readBuffer

			if nextDownload <= endBlock {
				c.Queue <- &InputData{path, nextDownload, bdNext.data, out}
				nextDownload += 1
			}

			//log.Infof("Added %v Length: %v", nextDownload, len(readBuffer.Bytes()))

			startOffHere := int64(0)
			endOffHere := int64(len(readBuffer.Bytes()))

			if bdNext.block == startBlock {
				startOffHere = startOff
			}

			if bdNext.block == endBlock && endOff < endOffHere {
				endOffHere = endOff
			}

			if endOffHere-startOffHere > 0 {
				//log.Debugf("Writing block %v", bdNext.block)
				size += endOffHere - startOffHere
				w.Write(readBuffer.Bytes()[startOffHere:endOffHere])
			}

			nextBlock += 1
			if nextDownload > endBlock {
				break
			}
		}
	}

	//log.Infof("Done adding blocks. Last added %v", nextDownload)

	for nextBlock < nextDownload {
		//log.Infof("%v %v", nextBlock, nextDownload)
		bdNext, ok := blocks[nextBlock]
		if !ok {
			//log.Infof("Waiting for block %v", nextBlock)
			bd := <-out
			//log.Infof("Got block %v", bd.block)
			blocks[bd.block] = bd
			continue
		}
		startOffHere := int64(0)
		endOffHere := int64(len(bdNext.data.Bytes()))

		if bdNext.block == startBlock {
			startOffHere = startOff
		}

		if bdNext.block == endBlock && endOff < endOffHere {
			endOffHere = endOff
		}

		if endOffHere-startOffHere > 0 {
			//log.Debugf("Writing block %v", bdNext.block)
			w.Write(bdNext.data.Bytes()[startOffHere:endOffHere])
			size += endOffHere - startOffHere
		}

		delete(blocks, nextBlock)
		nextBlock += 1
	}
	w.Close()
	elapsed := time.Since(startTime)
	fmt.Printf("Time: %v\tSize: %v\tSpeed: %v\n", elapsed, s3.ByteSize(size), s3.ByteSpeed(size, elapsed))
}

func (c *Client) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	return NewDownloadReadCloser(c, filePath, startAt, -1), nil
}

func (c *Client) OpenWriter(filePath string) (io.WriteCloser, error) {
	u := NewUploadWriteCloser(c, filePath)
	return u, nil
}

func (c *Client) ListFiles(dir string) (common.FileList, error) {
	//if len(dir) != 0 && !strings.HasPrefix(dir,"/") {
	//	dir = dir + "/"
	//}

	resp, err := makeRequest(fmt.Sprintf("http://%s/ls/%s", c.primaryAddr, dir), "GET")
	if err != nil {
		return common.FileList{}, err
	}
	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	io.Copy(buf, resp.Body)

	var fl common.FileList

	err = json.Unmarshal(buf.Bytes(), &fl)
	if err != nil {
		return common.FileList{}, err
	}
	return fl, nil
}

func (c *Client) Stat(filePath string) (common.FileInfo, error) {
	fi, ok := c.objectCache.Get(filePath)
	if ok {
		return fi.(common.FileInfo), nil
	}

	resp, err := makeRequest(fmt.Sprintf("http://%s/data/%s", c.primaryAddr, filePath), "HEAD")
	if err != nil {
		return common.FileInfo{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return common.FileInfo{}, errors.New("no file with given filename")
	}

	contentLength := resp.Header.Get("Content-Length")
	length, _ := strconv.ParseInt(contentLength, 10, 64)

	c.objectCache.Add(filePath, common.FileInfo{filePath, length})

	return common.FileInfo{filePath, length}, nil
}

func (c *Client) Delete(filename string) {
	resp, _ := makeRequest(fmt.Sprintf("http://%s/data/%s", c.primaryAddr, filename), "DELETE")
	resp.Body.Close()
}

// Split query sends `numSplits` chunks to make queries on them
func (c *Client) Query(path string, numSplits int64, condition string, col int, w io.Writer) error {
	fi, _ := c.Stat(path)
	chunkSize := (fi.Size + numSplits - 1)  / numSplits
	if chunkSize < c.BlockSize {
		chunkSize = c.BlockSize
	}

	min := func(x int64, y int64) int64 {
		if x < y {
			return x
		}
		return y
	}

	for offset := int64(0); offset < fi.Size; offset += chunkSize {
		resp, err := makeRangeRequest(fmt.Sprintf("http://%s/query/%s?col=%d&condition=%s", c.primaryAddr, path, col, condition),
			"GET", offset, min(offset + chunkSize - 1, fi.Size - 1))

		if err != nil {
			log.Fatal(err)
		}

		io.Copy(w, resp.Body)
		resp.Body.Close()
	}
	return nil
}