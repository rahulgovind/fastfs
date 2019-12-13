package api

import (
	"fmt"
	"github.com/rahulgovind/fastfs/metadatamanager"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type RPCServer struct {
	port uint16
}

func (s *RPCServer) SayHello(ctx context.Context, in *PingMessage) (*PingMessage, error) {
	return &PingMessage{Greeting: "bar"}, nil
}

func (s *RPCServer) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterFastFSServer(grpcServer, s)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

func NewServer(port uint16) *RPCServer {
	s := new(RPCServer)
	s.port = port

	return s
}
