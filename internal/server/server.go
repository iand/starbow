package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type Logger interface {
	Printf(format string, v ...interface{})
}

type Server struct {
	Addr   string
	Net    string
	Logger Logger
}

// New creates a new server with default values.
func New() *Server {
	return &Server{
		Addr:   ":2525",
		Net:    "tcp",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Serve starts the server and blocks until the process receives a terminating operating system signal.
func (s *Server) Serve() error {
	if s.Addr == "" {
		s.Addr = ":2525"
	}

	if s.Net == "" {
		s.Net = "tcp"
	}

	if s.Logger == nil {
		s.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	listener, err := net.Listen(s.Net, s.Addr)
	if err != nil {
		return fmt.Errorf("fatal error listening on %s: %v", s.Addr, err)
	}
	s.Logger.Printf("listening on %s", s.Addr)

	hs := &http.Server{
		Addr: s.Addr,
		// Handler:        Recover(s.router, s.Log),
	}

	go hs.Serve(listener)

	s.waitSignal()
	return nil
}

// waitSignal blocks waiting for operating system signals
func (s *Server) waitSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

signalloop:
	for sig := range ch {
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			break signalloop
			// TODO: support HUP
		}
	}
}
