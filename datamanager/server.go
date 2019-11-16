package datamanager

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"log"
	"net/http"
	"strings"
)

type Server struct {
	addr    string
	port    int
	handler func(path string, w io.Writer)
}

func NewServer(addr string, port int, handler func(path string, w io.Writer)) *Server {
	s := new(Server)
	s.addr = addr
	s.port = port
	s.handler = handler
	return s
}

type Handler struct {
	s *Server
}

func (h Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.s.ServeHTTP(w, req)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimLeft(req.RequestURI, "/")
	s.handler(path, w)

}

func (s *Server) Serve() {
	err := http.ListenAndServe(fmt.Sprintf(":%v", s.port), Handler{s})
	if err != nil {
		log.Fatal(err)
	}
}

type FastServer struct {
	addr    string
	port    int
	handler func(path string, w io.Writer)
}

func NewFastServer(addr string, port int, handler func(path string, w io.Writer)) *FastServer {
	s := new(FastServer)
	s.addr = addr
	s.port = port
	s.handler = handler
	return s
}

//
//type FastHandler struct {
//	s *FastServer
//}
//
//func (h FastHandler) HandleFastHTTP(ctx *fasthttp.RequestCtx) {
//	h.s.HandleFastHTTP(ctx)
//}

func (s *FastServer) HandleFastHTTP(ctx *fasthttp.RequestCtx) {
	path := strings.TrimLeft(string(ctx.RequestURI()), "/")
	s.handler(path, ctx)
}

func (s *FastServer) Serve() {
	err := fasthttp.ListenAndServe(fmt.Sprintf(":%v", s.port), s.HandleFastHTTP)
	if err != nil {
		log.Fatal(err)
	}
}
