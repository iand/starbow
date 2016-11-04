package collation

import (
	"encoding/binary"
	"testing"

	"github.com/iand/starbow/internal/summary/stat64"
)

func TestCompileRecordMeasure(t *testing.T) {
	rowCounter := Schema{
		Name:           "rowcounter",
		RecordMeasures: []RowMeasure{Count{}},
	}

	coll, err := rowCounter.Compile()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := Row{}

	buf := make([]byte, 8)
	for i := 0; i < 15; i++ {
		err := coll.Update(r, buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	count := binary.LittleEndian.Uint64(buf[:8])
	if count != 15 {
		t.Fatalf("got %v, wanted %v", count, 15)
	}
}

func TestCompileWithMeasure(t *testing.T) {
	fooCounter := Schema{
		Name: "foocounter",
		Measures: []MeasureSpec{
			{
				Field:    FieldSpec{Pattern: "foo"},
				Measures: []Measure{Count{}, Sum{}},
			},
		},
	}

	coll, err := fooCounter.Compile()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := Row{
		Data: []FV{
			{F: []byte("foo"), V: []byte("4")},
		},
	}

	buf := make([]byte, 32)
	for i := 0; i < 15; i++ {
		err := coll.Update(r, buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	stat := stat64.New(buf)

	count := stat.Count()
	if count != 15 {
		t.Fatalf("got count %v, wanted %v", count, 15)
	}

	sum := stat.Sum()
	if sum != 60 {
		t.Fatalf("got sum %v, wanted %v", sum, 60)
	}

}

func TestCompileUsesSameSummaryForCountSumMeanVariance(t *testing.T) {
	sch := Schema{
		Measures: []MeasureSpec{
			{
				Field:    FieldSpec{Pattern: "foo"},
				Measures: []Measure{Count{}, Sum{}, Mean{}, Variance{}},
			},
		},
	}

	coll, err := sch.Compile()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ws, _ := coll.contWriters["foo"]
	if len(ws) != 1 {
		t.Errorf("got %v writers, wanted 1", len(ws))
	}

	rows := []Row{
		{
			Data: []FV{
				{F: []byte("foo"), V: []byte("4")},
			},
		},

		{
			Data: []FV{
				{F: []byte("foo"), V: []byte("2")},
			},
		},

		{
			Data: []FV{
				{F: []byte("foo"), V: []byte("9")},
			},
		},
	}

	buf := make([]byte, 32)
	for _, r := range rows {
		if err := coll.Update(r, buf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	stat := stat64.New(buf)

	count := stat.Count()
	if count != 3 {
		t.Fatalf("got count %v, wanted %v", count, 3)
	}

	sum := stat.Sum()
	if sum != 15 {
		t.Fatalf("got sum %v, wanted %v", sum, 15)
	}

	mean := stat.Mean()
	if mean != 5 {
		t.Fatalf("got mean %v, wanted %v", mean, 5)
	}

	variance := stat.Variance()
	if variance != 13 {
		t.Fatalf("got variance %v, wanted %v", variance, 13)
	}

}
