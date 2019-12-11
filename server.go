package main

import (
	"bytes"
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
	s.localClient = NewClient(s.localAddress, dm.BlockSize)
	s.fastfs = fastfs
	return s
}

func (s *Server) rangeHandler(path string, w io.Writer, start int64, end int64) {

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

	ag := datamanager.NewAggregator(int(numThreads), path, int(start), int(end), s.localClient)
	ag.WriteTo(w)
}

type FileResponse struct {
	Filename string
	FileSize int
}

type LsResponse struct {
	Files []FileResponse
}

type SetupResponse struct {
	Servers   []string
	BlockSize int
}

func getWriterWraper(w http.ResponseWriter, encoding string) io.Writer {
	if encoding == "gzip" {
		log.Info("Using gzip compression")
		w.Header().Set("Content-Encoding", "gzip")
		//buf := bufio.NewWriterSize(w, 1024*1024)
		return pgzip.NewWriter(w)
	} else if encoding == "deflate" {
		//log.Info("Using deflate compression")
		return w
		//w.Header().Set("Content-Encoding", "deflate")
		//buf := bufio.NewWriterSize(w, 1024 * 1024)
		//writer, _ := flate.NewWriter(ioutil.Discard, -1)
		//return writer
	} else {
		return w
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

	s.rangeHandler(path, buf, startOffset, end)
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
	fmt.Println("Processed query")
	if len(bts) > 0 {
		w.Write(bts)
	}
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

	fmt.Println(req.Method)
	if req.Method == "HEAD" {
		file := s.mm.Query(path)
		if file == nil {
			w.WriteHeader(404)
			return
		}

		w.Header().Set("Content-Length", fmt.Sprintf("%v", file.FileSize))
		return
	}

	fmt.Println("Accept-Encoding: ", req.Header.Get("Accept-Encoding"))

	if cmd == "query" {
		p := req.Header.Get("Range")
		fmt.Println("Content-Range: ", p)
		col := req.URL.Query().Get("col")
		colNum, _ := strconv.ParseInt(col, 10, 64)

		condition := req.URL.Query().Get("condition")
		s.queryHandler(path, w, 0, -1, condition, colNum)
		return
	}

	if cmd == "data" {
		block := req.URL.Query().Get("block")
		force := req.URL.Query().Get("force")
		p := req.Header.Get("Range")
		fmt.Println("Content-Range: ", p)
		if block == "" {
			encodingHeader := req.Header.Get("Accept-Encoding")
			compression := ""
			log.Infof("Encoding Header: %v", encodingHeader)
			if strings.Contains(encodingHeader, "gzip") {
				compression = "gzip"
			} else if strings.Contains(encodingHeader, "deflate") {
				compression = "deflate"
			}

			rangeString := req.Header.Get("Range")
			if rangeString == "" {
				if compression == "gzip" {
					w2 := pgzip.NewWriter(w)
					s.rangeHandler(path, w2, 0, -1)
					w2.Close()
					return
				}
				s.rangeHandler(path, getWriterWraper(w, compression), 0, -1)
				return
			}
			ranges, err := parseRange(rangeString, math.MaxInt64)
			fmt.Println("ranges: ", ranges)

			if err != nil {
				fmt.Println(err)
				http.Error(w, "Requested Range Not Satisfiable", 416)
				return
			}
			start := ranges[0].start
			length := ranges[0].length

			w.Header().Set("Content-Range", getRange(start, start+length-1, -1))
			w.WriteHeader(206)
			s.rangeHandler(path, getWriterWraper(w, compression), start, start+length-1)
		} else {

			blockNum, err := strconv.ParseInt(block, 10, 32)
			target := s.partitioner.GetServer(path, int(blockNum))

			fmt.Println("Target: ", target, "\tCurr: ", s.localAddress)
			if target != s.localAddress && force != "1" {
				// Need to redirect :(
				http.Redirect(w, req, fmt.Sprintf("http://%s/data/%s?block=%d&force=1", target, path, blockNum),
					http.StatusMovedPermanently)
				return
			}

			if err != nil {
				log.Error(err)
			}

			data, err := s.dm.Get(path, int(blockNum))
			if err != nil {
				log.Fatal(err)
			}
			w.Write(data)
		}
		return
	} else if cmd == "ls" {
		fmt.Println(path)
		nodes := s.mm.Stat(path)
		var output LsResponse
		for _, node := range nodes {
			output.Files = append(output.Files, FileResponse{
				Filename: node.Key,
				FileSize: int(node.FileSize),
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

func (s *Server) Serve() {
	err := http.ListenAndServe(s.localAddress, s)
	if err != nil {
		log.Fatal(err)
	}
}
