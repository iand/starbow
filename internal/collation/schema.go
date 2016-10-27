package collation

import (
	"bytes"
	"strconv"
	"time"

	"github.com/iand/starbow/internal/hash/fnv"
)

// A Schema defines the structure of a collation.
type Schema struct {
	Name           string
	Filter         Filter
	Keys           []KeySpec
	Measures       []MeasureSpec // Field-level measures
	RecordMeasures Measures      // Record-level measures
}

// A Filter specifies criteria by which a record is selected.
type Filter struct {
}

// A KeySpec specifies a key field in a schema.
type KeySpec struct {
	Field FieldSpec
}

// A FieldSpec specifies a field contained in an incoming observation.
type FieldSpec struct {
	Pattern string
}

// A MeasureSpec specifies a measure field in a schema.
type MeasureSpec struct {
	Field    FieldSpec
	Measures Measures
}

// Row is a row of data to be collated containing fields and their values.
type Row struct {
	ReceiveTime time.Time // received time
	DataTime    time.Time // data time
	Data        []FV      // fields and values in the row. Field names must be unique.
}

// FV is a field name with a value
type FV struct {
	F, V []byte // field name and value
}

// KeyValue builds a key value by locating Row fields corresponding to the key names supplied
// and hashing their names and values. It returns false as the second argument if the key could not be
// constructed, e.g. if one of the keys does not match a field in the row. keys must be sorted
// in ascending order.
func (r *Row) KeyValue(keys [][]byte) (uint64, bool) {
	h := fnv.New64()
	// TODO: optimize naive key search
outer:
	for i := range keys {
		for j := range r.Data {
			if bytes.Equal(keys[i], r.Data[j].F) {
				h.Write(r.Data[j].F)
				h.Write(r.Data[j].V)
				continue outer
			}
		}
		return 0, false
	}

	return h.Sum64(), true
}

// Reset clears the row of all data.
func (r *Row) Reset() {
	r.ReceiveTime = time.Time{}
	r.DataTime = time.Time{}
	r.Data = r.Data[:0]
}

type Collator struct {
	keys        [][]byte
	writers     []anyWriter
	contWriters map[string][]contWriter
	size        int // Size of buffer required to write all measures, i.e. record size
}

// RowUpdate reads the row and updates buf according to the schema's measures.
// buf must be long enough for the schema's data.
func (c Collator) RowUpdate(r Row, buf []byte) error {
	for _, w := range c.writers {
		if err := w.Update(buf); err != nil {
			return err
		}
	}

	for _, fv := range r.Data {
		if ws, exists := c.contWriters[string(fv.F)]; exists {
			v, err := strconv.ParseFloat(string(fv.V), 64)
			if err != nil {
				return err
			}
			for _, w := range ws {
				if err := w.UpdateFloat64(buf, v); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c Collator) Keys() [][]byte {
	return c.keys
}

type anyWriter struct {
	Low, High int // index range of data within buffer
	Fn        func([]byte) error
}

func (a anyWriter) Update(buf []byte) error {
	return a.Fn(buf[a.Low:a.High])
}

type contWriter struct {
	Low, High int // index range of data within buffer
	Fn        func(buf []byte, v float64) error
}

func (c contWriter) UpdateFloat64(buf []byte, v float64) error {
	return c.Fn(buf[c.Low:c.High], v)
}
