package api

import (
	"fmt"
	"github.com/rahulgovind/fastfs/metadatamanager"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	port uint16
	mm   *metadatamanager.MetadataManager
}

func (s *Server) SayHello(ctx context.Context, in *PingMessage) (*PingMessage, error) {
	return &PingMessage{Greeting: "bar"}, nil
}

func (s *Server) OpenFile(ctx context.Context, in *OpenFileMessage) (*OpenFileResponse, error) {
	version, _ := s.mm.Open(in.Path)
	return &OpenFileResponse{Version: version}, nil
}

func (s *Server) ListFiles(ctx context.Context, in *FileQuery) (*FileListResponse, error) {
	s.mm.LoadFS()
	l := s.mm.GetFiles()
	return &FileListResponse{Filenames: l}, nil
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterFastFSServer(grpcServer, s)
	s.mm.LoadFS()
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

func NewServer(port uint16, bucket string) *Server {
	s := new(Server)
	s.port = port
	s.mm = metadatamanager.NewMetadataManager(bucket)

	return s
}
