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
	"github.com/iand/starbow/internal/query"
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

	mux *http.ServeMux
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

	if err := s.registerHandlers(); err != nil {
		return fmt.Errorf("fatal error registering handlers: %v", err)
	}

	hs := &http.Server{
		Addr:    s.Addr,
		Handler: s,
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

func (s *Server) registerHandlers() error {
	s.mux = http.NewServeMux()

	s.mux.HandleFunc("/obs", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			baseCtx := req.Context()
			s.recvObservation(baseCtx, w, req)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	s.mux.Handle("/collation/", http.StripPrefix("/collation/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path := req.URL.Path

		if len(path) == 0 {
			// TODO: list collations
			http.NotFound(w, req)
			return
		}

		if strings.ContainsRune(path, '/') {
			http.NotFound(w, req)
			return
		}

		if req.Method != http.MethodPost && req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		baseCtx := req.Context()
		s.queryCollation(baseCtx, path, w, req)
	})))

	return nil
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
		http.Error(w, "unknown media type", http.StatusUnsupportedMediaType)
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

// queryCollation parses a query from the request and performs it on the named collation.
func (s *Server) queryCollation(ctx context.Context, cname string, w http.ResponseWriter, req *http.Request) {

	qstr := req.FormValue("q")
	q, err := query.RoughParse(qstr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, store := range s.Stores {
		if store.Collator.Name() == cname {
			res, err := store.Read(ctx, q)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			for _, c := range q.Criteria {
				w.Write(c.F)
				w.Write([]byte{'='})
				w.Write(c.V)
				w.Write([]byte{'\n'})
			}

			for _, f := range res.FieldMeasureValues {
				w.Write([]byte(f.String()))
				w.Write([]byte{'\n'})
			}
			return
		}
	}

	http.NotFound(w, req)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	s.mux.ServeHTTP(w, r)
}
