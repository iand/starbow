package main

import (
	"github.com/iand/starbow/internal/bufmap"
	"github.com/iand/starbow/internal/collation"
	"github.com/iand/starbow/internal/server"
	"github.com/iand/starbow/internal/storage"
)

func setupDemo(s *server.Server) error {
	rowCounter := collation.Schema{
		Name:           "rowcounter",
		RecordMeasures: []collation.RowMeasure{collation.Count{}},
	}

	collCounter, err := rowCounter.Compile()
	if err != nil {
		return err
	}

	storeCounter := storage.Store{
		Backend:  bufmap.New(5000, collCounter.Size()),
		Collator: collCounter,
	}

	s.Stores = append(s.Stores, storeCounter)

	return nil
}
