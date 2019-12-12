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
	"github.com/rahulgovind/fastfs/s3"
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
	mm           *s3.MetadataManager
	partitioner  Partitioner
	localClient  *Client
	localAddress string
	fastfs       *FastFS
}

func NewServer(addr string, port int, dm *datamanager.DataManager, mm *s3.MetadataManager, p Partitioner,
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
	file := s.mm.Query(path)
	if file == nil {
		return -1
	}
	return file.FileSize
}

func (s *Server) handleHead(w http.ResponseWriter, req *http.Request, path string) {
	file := s.mm.Query(path)
	if file == nil {
		w.WriteHeader(404)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%v", file.FileSize))
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

	if req.Method == "HEAD" {
		s.handleHead(w, req, path)
		return
	}

	if req.Method == "PUT" {
		s.handlePut(w, req, path)
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
			encodingHeader := req.Header.Get("Accept-Encoding")
			compression := ""

			if strings.Contains(encodingHeader, "gzip") {
				compression = "gzip"
			} else if strings.Contains(encodingHeader, "deflate") {
				compression = "deflate"
			}

			rangeString := req.Header.Get("Range")
			fmt.Println("Range string: ", rangeString)
			if rangeString == "" {
				s.rangeHandler(path, getWriterWraper(w, compression), 0, -1)
				return
			}

			//fileSize := s.getFileSize(path)
			//if fileSize == -1 {
			//	w.WriteHeader(404)
			//}

			ranges, err := parseRange(rangeString, 1024)
			if err != nil {
				log.Error(err)
				http.Error(w, "Requested Range Not Satisfiable", 416)
				return
			}
			start := ranges[0].start
			length := ranges[0].length

			// Redirect?
			if force != "1" {
				target := s.partitioner.GetServer(path, int(start/s.localClient.BlockSize))

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
			s.rangeHandler(path, getWriterWraper(w, compression), start, start+length-1)
		} else {

			blockNum, err := strconv.ParseInt(block, 10, 32)
			target := s.partitioner.GetServer(path, int(blockNum))

			if target != s.localAddress && force != "1" {
				// Need to redirect :(
				http.Redirect(w, req, fmt.Sprintf("http://%s/data/%s?block=%d&force=1", target, path, blockNum),
					http.StatusMovedPermanently)
				return
			}

			if err != nil {
				log.Error(err)
			}

			data, err := s.dm.Get(path, blockNum)
			if err != nil {
				log.Fatal(err)
			}
			w.Write(data)
		}
		return
	} else if cmd == "ls" {
		nodes := s.mm.Stat(path)
		var output LsResponse
		for _, node := range nodes {
			fileSize := int64(-1)
			if !node.IsDir {
				fileSize = node.FileSize
			}
			output.Files = append(output.Files, FileResponse{
				Filename: node.Key,
				FileSize: fileSize,
			})
		}

		res, _ := json.Marshal(output)
		w.Write(res)
		return
	} else if cmd == "setup" {
		data := SetupResponse{
			Servers:   s.fastfs.GetServers(),
			BlockSize: s.dm.BlockSize,
		}
		res, _ := json.Marshal(data)
		w.Write(res)
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
		}
		buf := bytes.NewBuffer(nil)
		io.Copy(buf, req.Body)
		s.dm.Put(path, blockNum, buf.Bytes())
		return
	}

	log.Info("Received PUT request for ", path)
	reader, writer := io.Pipe()

	rag := datamanager.NewReverseAggregator(path, s.localClient, writer, 16)

	go s.dm.Upload(path, reader)
	rag.ReadFrom(req.Body)

	req.Body.Close()

	log.Info("Done copying data")
}

func (s *Server) Serve() {
	err := http.ListenAndServe(s.localAddress, s)
	if err != nil {
		log.Fatal(err)
	}
}
