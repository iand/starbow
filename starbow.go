package main

import (
	"log"

	"github.com/iand/starbow/internal/server"
)

func main() {
	s := server.New()
	if err := setupDemo(s); err != nil {
		log.Fatal(err.Error())
	}
	s.Serve()
}
