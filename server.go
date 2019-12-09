package main

import (
	"encoding/json"
	"fmt"
	"github.com/rahulgovind/fastfs/datamanager"
	"github.com/rahulgovind/fastfs/s3"
	log "github.com/sirupsen/logrus"
	"io"
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

func (s *Server) rangeHandler(path string, w io.Writer) {
	ag := datamanager.NewAggregator(5, path, s.dm)
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

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	query := strings.TrimLeft(req.URL.Path, "/")
	firstSlash := strings.Index(query, "/")
	cmd := query
	var path string

	if firstSlash != -1 {
		cmd = query[:firstSlash]
		path = query[firstSlash+1:]
	}

	if cmd == "data" {
		block := req.URL.Query().Get("block")
		force := req.URL.Query().Get("force")

		if block == "" {
			s.rangeHandler(path, w)
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
