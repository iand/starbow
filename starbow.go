package main

import (
	"github.com/iand/starbow/internal/server"
)

func main() {
	s := server.New()
	s.Serve()
}
