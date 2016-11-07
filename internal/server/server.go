// Package server implements a starbow node.
package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/iand/starbow/internal/collation"
	"github.com/iand/starbow/internal/storage"
)

type Logger interface {
	Printf(format string, v ...interface{})
}

type Server struct {
	Addr   string
	Net    string
	Logger Logger
	Stores []storage.Store
}

// New creates a new server with default values.
func New() *Server {
	return &Server{
		Addr:   ":2525",
		Net:    "tcp",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		Stores: make([]storage.Store, 0),
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
		Addr:    s.Addr,
		Handler: http.HandlerFunc(s.route),
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
			// TODO: support HUP to reload config
		}
	}
}

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation.
type contextKey struct {
	name string
}

func (s *Server) route(w http.ResponseWriter, req *http.Request) {
	baseCtx := req.Context()

	path := req.URL.Path

	switch {
	case strings.HasPrefix(path, "/obs"):
		if req.Method == http.MethodPost {
			s.recvObservation(baseCtx, w, req)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	http.NotFound(w, req)
}

// recvObservation receives one or more observations and sends them to the collators
func (s *Server) recvObservation(ctx context.Context, w http.ResponseWriter, req *http.Request) {
	ct, err := contentType(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var r io.Reader
	switch ct {
	case "application/x-www-form-urlencoded":
		data := req.FormValue("data")
		if data == "" {
			http.Error(w, "no data supplied", http.StatusBadRequest)
			return
		}
		r = strings.NewReader(data)
	case "text/plain", "application/octet-stream":
		r = req.Body
		defer req.Body.Close()
	default:
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}

	now := time.Now()
	sc := bufio.NewScanner(r)
	for sc.Scan() {

		// Check whether context has been cancelled
		select {
		case <-ctx.Done():
			return
		default:
		}

		row, err := parseRow(sc.Bytes())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		row.ReceiveTime = now
		fmt.Printf("%+v\n", row)
		for _, st := range s.Stores {
			if err := st.Write(ctx, row); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	if sc.Err() != nil {
		http.Error(w, sc.Err().Error(), http.StatusInternalServerError)
		return
	}
}

func contentType(req *http.Request) (string, error) {
	ct := req.Header.Get("Content-Type")
	if ct == "" {
		return "application/octet-stream", nil
	}
	var err error
	ct, _, err = mime.ParseMediaType(ct)
	return ct, err
}

// TODO: move parseRow into a dedicated text format parser
func parseRow(data []byte) (collation.Row, error) {
	row := collation.Row{}
	pmax := 20
	if len(data) < pmax {
		pmax = len(data)
	}
	var p int
	// Read timestamp
	for p = 0; p < pmax; p++ {
		if data[p] < '0' || data[p] > '9' {
			break
		}
	}

	if p == 0 {
		// No timestamp
		return row, fmt.Errorf("row missing leading timestamp")
	}

	// Timestamp is in microseconds
	ts, err := strconv.Atoi(string(data[:p]))
	if err != nil {
		return row, err
	}

	row.DataTime = time.Unix(0, int64(ts)*1000)

	if p == len(data) {
		// No fields or values
		return row, nil
	}

	// First byte after the timestamp is the delimiter for the rest of the row
	var delim = byte('\t')
	delim = data[p]
	p++

	for {
		offset := bytes.IndexByte(data[p:], delim)
		if offset == -1 {
			row.Data = append(row.Data, parseFV(data[p:]))
			break
		}

		row.Data = append(row.Data, parseFV(data[p:p+offset]))
		p += offset + 1
	}

	return row, nil
}

func parseFV(data []byte) collation.FV {
	eq := bytes.IndexByte(data, '=')
	if eq == -1 {
		return collation.FV{
			F: data,
			V: []byte{},
		}
	}
	return collation.FV{
		F: data[:eq],
		V: data[eq+1:],
	}
}
