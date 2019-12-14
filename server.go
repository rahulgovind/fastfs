package main

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"github.com/klauspost/pgzip"
	"github.com/minio/select-simd"
	"github.com/rahulgovind/fastfs/csvutils"
	"github.com/rahulgovind/fastfs/datamanager"
	"github.com/rahulgovind/fastfs/metadatamanager"
	"github.com/rahulgovind/fastfs/partitioner"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	addr         string
	port         int
	blockHandler func(path string, block int, w io.Writer)
	dm           *datamanager.DataManager
	mm           *metadatamanager.MetadataManager
	partitioner  partitioner.Partitioner
	localClient  *Client
	localAddress string
	fastfs       *FastFS
}

func NewServer(addr string, port int, dm *datamanager.DataManager, mm *metadatamanager.MetadataManager,
	p partitioner.Partitioner,
	fastfs *FastFS) *Server {
	s := new(Server)
	s.addr = addr
	s.port = port
	s.dm = dm
	s.mm = mm
	s.partitioner = p
	s.localAddress = fmt.Sprintf("%s:%d", addr, port)
	s.localClient = NewClient(s.localAddress, dm.BlockSize, dm, p)
	s.fastfs = fastfs
	return s
}

func (s *Server) rangeHandler(path string, w io.WriteCloser, start int64, end int64) {

	startTime := time.Now()
	if end == -1 {
		end = math.MaxInt32
	}

	startBlock := start / int64(s.localClient.BlockSize)
	endBlock := end / int64(s.localClient.BlockSize)

	if end%int64(s.localClient.BlockSize) == 0 {
		endBlock -= 1
	}

	numThreads := int64(16)
	if endBlock-startBlock+int64(1) < numThreads {
		numThreads = endBlock - startBlock + 1
	}

	ag := datamanager.NewAggregator(int(numThreads), path, start, end, s.localClient)
	ag.WriteTo(w)
	elapsed := time.Since(startTime)
	fmt.Printf("Range download took %v", elapsed)
}

type FileResponse struct {
	Filename string
	FileSize int64
}

type LsResponse struct {
	Files []FileResponse
}

type SetupResponse struct {
	Servers   []string
	BlockSize int64
}

func getWriterWraper(w http.ResponseWriter, encoding string) io.WriteCloser {
	if encoding == "gzip" {
		log.Info("Using gzip compression")
		w.Header().Set("Content-Encoding", "gzip")
		return pgzip.NewWriter(w)
	} else if encoding == "deflate" {
		log.Info("Using deflate compression")
		w.Header().Set("Content-Encoding", "deflate")
		writer, _ := flate.NewWriter(w, -1)
		return writer
	} else {
		return &datamanager.FakeWriteCloser{w}
	}
}

func (s *Server) queryHandler(path string, w http.ResponseWriter, start int64, end int64,
	condition string, col int64) {
	blockSize := s.localClient.BlockSize

	buf := bytes.NewBuffer(nil)
	startOffset := start - int64(s.localClient.BlockSize)
	startSkip := start == 0

	if startOffset < 0 {
		startOffset = 0
	}

	s.rangeHandler(path, &datamanager.FakeWriteCloser{buf}, startOffset, end)
	raw := csvutils.AlignedSlices(buf.Bytes(), startSkip, blockSize)
	rows := bytes.Count(buf.Bytes(), []byte{0x0a}) * 2

	rowIndices := make([]uint32, rows)
	maskCommas := make([]uint64, (len(raw)+63)>>6)
	colIndices := make([]uint32, rows)
	equal := make([]uint64, (rows+63)>>6)

	r := selectsimd.ParseCsvAdjusted(raw, rowIndices, maskCommas)
	selectsimd.ExtractIndexForColumn(maskCommas, rowIndices[:r], colIndices[:r], uint64(col))
	selectsimd.EvaluateCompareString(raw, colIndices[:r], condition, equal[:(r+63)>>6])
	bts := selectsimd.ExtractCsv(equal[:(r+63)>>6], raw, rowIndices[:r])

	log.Debug("Processed query")

	if len(bts) > 0 {
		w.Write(bts)
	}
}

func (s *Server) getFileSize(path string) int64 {
	file, err := s.mm.Query(path)
	if err == metadatamanager.FileNotFoundError {
		return -1
	}
	return file.Size
}

func (s *Server) handleHead(w http.ResponseWriter, req *http.Request, path string) {
	file, err := s.mm.Query(path)
	if err == metadatamanager.FileNotFoundError {
		w.WriteHeader(404)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%v", file.Size))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "binary/octet-stream")
	log.Debug("length: ", w.Header().Get("Content-Length"))
	return
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	query := strings.TrimLeft(req.URL.Path, "/")
	firstSlash := strings.Index(query, "/")
	cmd := query
	var path string

	if firstSlash != -1 {
		cmd = query[:firstSlash]
		path = query[firstSlash+1:]
	}

	fmt.Println("Got: ", req.Method, req.RequestURI)
	if req.Method == "HEAD" {
		s.handleHead(w, req, path)
		return
	}

	if req.Method == "PUT"  || req.Method == "POST" || cmd == "put" {
		s.handlePut(w, req, path)
		return
	}

	if req.Method == "DELETE" {
		s.handleDelete(w, req, path)
		return
	}

	if cmd == "query" {
		col := req.URL.Query().Get("col")
		colNum, _ := strconv.ParseInt(col, 10, 64)

		condition := req.URL.Query().Get("condition")
		s.queryHandler(path, w, 0, -1, condition, colNum)
		return
	}

	if cmd == "data" {
		w.Header().Set("Accept-Ranges", "bytes")

		block := req.URL.Query().Get("block")
		force := req.URL.Query().Get("force")


		if block == "" {

			// Compressiong
			dataWriter := s.getCompressionWriter(w, req)
			defer dataWriter.Close()

			rangeString := req.Header.Get("Range")
			fmt.Println("Range string: ", rangeString)
			if rangeString == "" {
				s.rangeHandler(path, dataWriter, 0, -1)
				log.Error("Done writing")
				return
			}

			//fileSize := s.getFileSize(path)
			//if fileSize == -1 {
			//	w.WriteHeader(404)
			//}

			ranges, err := parseRange(rangeString, math.MaxInt64>>3)
			if err != nil {
				log.Error(err)
				http.Error(w, "Requested Range Not Satisfiable", 416)
				return
			}
			start := ranges[0].start
			length := ranges[0].length

			// Redirect?
			if force != "1" {
				target := s.partitioner.GetServer(path, start/s.localClient.BlockSize)

				fmt.Println(target, s.localAddress)
				if target != s.localAddress {
					// Bye bye
					http.Redirect(w, req, fmt.Sprintf("http://%s/data/%s?force=1", target, path),
						http.StatusMovedPermanently)
					return
				}
			}
			w.WriteHeader(206)
			fmt.Println("Range parameters: ", start, length)
			s.rangeHandler(path, dataWriter, start, start+length-1)
		} else {

			// We don't use compression internally
			dataWriter := &datamanager.FakeWriteCloser{w}
			defer dataWriter.Close()

			blockNum, err := strconv.ParseInt(block, 10, 32)
			if force != "1" {
				target := s.partitioner.GetServer(path, blockNum)

				if s.localAddress != target {
					req.URL.Host = target
					http.Redirect(w, req, req.URL.String(), http.StatusMovedPermanently)
					return
				}
			}
			onlyCache := req.URL.Query().Get("onlyCache") == "true"

			data, ok := s.dm.CacheGet(path, blockNum)

			if ok {
				_, err = dataWriter.Write(data)
				if err != nil {
					log.Fatal(err)
				}

				if onlyCache {
					s.dm.CacheDelete(path, blockNum)
				}
				return
			}

			if onlyCache {
				w.WriteHeader(404)
				return
			}

			// Nope. I don't this data. Let's ask the metadata registry if someone else has a copy
			candidate, ok := s.mm.QueryLocation(path, blockNum)

			// If I am the candidate then it looks like a parallel request went thorugh
			// or I have deleted the file. Either way, download again.
			if ok && candidate != s.localAddress {
				log.Error("I don't have block but looks like someone else might")
				// Someone else probably has a copy. Fetch it and ask them to delete it.
				data, err = s.localClient.DirectGet(path, blockNum, candidate, true)
				if err == nil {
					log.Error("Started copying\t", blockNum)
					s.dm.CachePut(path, blockNum, data)
					_, err = dataWriter.Write(data)
					if err != nil {
						log.Fatal(err)
					}
					log.Error("Done writing\t", blockNum)
					return
				}
			}

			// No one else has it. Just fetch it from S3 lol
			fmt.Println("Block hit miss. Fetching from S3")
			data, err = s.dm.Get(path, blockNum)
			if err != nil {
				log.Fatal(err)
			}

			_, err = dataWriter.Write(data)
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	} else if cmd == "ls" {
		fl, _ := s.mm.GetList(path)
		res, _ := json.Marshal(fl)
		_, err := w.Write(res)
		if err != nil {
			log.Fatal(err)
		}
		return
	} else if cmd == "setup" {
		data := SetupResponse{
			Servers:   s.fastfs.GetServers(),
			BlockSize: s.dm.BlockSize,
		}
		res, _ := json.Marshal(data)
		_, err := w.Write(res)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	w.WriteHeader(404)
	return
}

func (s *Server) handlePut(w http.ResponseWriter, req *http.Request, path string) {
	block := req.URL.Query().Get("block")
	if block != "" {
		log.Info("Receiving put request to cache")
		blockNum, err := strconv.ParseInt(block, 10, 32)
		if err != nil {
			log.Error(err)
			w.WriteHeader(500)
			return
		}

		log.Infof("Receiving disaggregated block %v %v", path, blockNum)
		buf := bytes.NewBuffer(nil)
		io.Copy(buf, req.Body)
		s.dm.CachePut(path, blockNum, buf.Bytes())
		return
	}

	//s.dm.Upload(path, req.Body)
	rag := s.dm.NewReverseAggregator(path, req.Body, 16)

	s.dm.Upload(path, rag)
	req.Body.Close()

	log.Info("Done copying data")
}

func (s *Server) handleDelete(w http.ResponseWriter, req *http.Request, path string) {
	log.Info("Deleting ", path)
	s.dm.Delete(path)
}

func (s *Server) Serve() {
	err := http.ListenAndServe(s.localAddress, s)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) getCompressionWriter(w http.ResponseWriter, req *http.Request) io.WriteCloser {
	compression := ""
	encodingHeader := req.Header.Get("Accept-Encoding")
	if strings.Contains(encodingHeader, "gzip") {
		compression = "gzip"
	} else if strings.Contains(encodingHeader, "deflate") {
		compression = "deflate"
	}
	return getWriterWraper(w, compression)
}