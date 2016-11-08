package main

import (
	"github.com/iand/starbow/internal/bufmap"
	"github.com/iand/starbow/internal/collation"
	"github.com/iand/starbow/internal/server"
	"github.com/iand/starbow/internal/storage"
)

func setupDemo(s *server.Server) error {
	addDemoCollation(s, collation.Schema{
		Name: "country",
		Keys: []collation.KeySpec{
			{Field: collation.FieldSpec{Pattern: "country"}},
		},
		RecordMeasures: []collation.RowMeasure{collation.Count{}},
		Measures: []collation.MeasureSpec{
			{
				Field:    collation.FieldSpec{Pattern: "height"},
				Measures: []collation.Measure{collation.Mean{}, collation.Sum{}, collation.Variance{}, collation.Max{}, collation.Min{}},
			},
		},
	})

	addDemoCollation(s, collation.Schema{
		Name: "tz",
		Keys: []collation.KeySpec{
			{Field: collation.FieldSpec{Pattern: "tz"}},
		},
		RecordMeasures: []collation.RowMeasure{collation.Count{}},
		Measures: []collation.MeasureSpec{
			{
				Field:    collation.FieldSpec{Pattern: "height"},
				Measures: []collation.Measure{collation.Mean{}, collation.Sum{}, collation.Variance{}, collation.Max{}, collation.Min{}},
			},
		},
	})

	// addDemoCollation(s, collation.Schema{
	// 	Name: "baruniques",
	// 	Keys: []collation.KeySpec{
	// 		{Field: collation.FieldSpec{Pattern: "x"}},
	// 	},
	// 	Measures: []collation.MeasureSpec{
	// 		{
	// 			Field:    collation.FieldSpec{Pattern: "bar"},
	// 			Measures: []collation.Measure{collation.Cardinality{Precision: 14}},
	// 		},
	// 	},
	// })

	return nil
}

func addDemoCollation(s *server.Server, sch collation.Schema) error {
	coll, err := sch.Compile()
	if err != nil {
		return err
	}

	store := storage.Store{
		Backend:  bufmap.New(5000, coll.Size()),
		Collator: coll,
	}

	s.Stores = append(s.Stores, store)
	return nil
}
