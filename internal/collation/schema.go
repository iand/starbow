package collation

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/iand/starbow/internal/hash/fnv"
)

var (
	ErrMeasureNotFound = errors.New("measure not found for field")
)

const (
	RowPseudoField = "*" // a field name that represents a row level measure
)

// A Schema defines the structure of a collation.
type Schema struct {
	Name           string
	Filter         Filter
	Keys           []KeySpec
	Measures       []MeasureSpec // Field-level measures
	RecordMeasures []RowMeasure  // Record-level measures
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
	Measures []Measure
}

// Row is a row of data to be collated containing fields and their values.
type Row struct {
	ReceiveTime time.Time // received time
	DataTime    time.Time // data time
	Data        FVList    // fields and values in the row. Field names must be unique.
}

// FV is a field name with a value
type FV struct {
	F, V []byte // field name and value
}

type FVList []FV

// KeyValue builds a key value by locating Query criteria fields corresponding
// to the key names supplied and hashing their names and values. It returns
// false as the second argument if the key could not be constructed, e.g. if
// one of the keys does not match a field in the query criteria. keys must be
// sorted in ascending order.
func (fv FVList) KeyValue(keys [][]byte) (uint64, bool) {
	h := fnv.New64()
	// TODO: optimize naive key search
outer:
	for i := range keys {
		for j := range fv {
			if bytes.Equal(keys[i], fv[j].F) {
				h.Write(fv[j].F)
				h.Write(fv[j].V)
				continue outer
			}
		}
		return 0, false
	}

	return h.Sum64(), true
}

// KeyValue builds a key value by locating Row fields corresponding to the key names supplied
// and hashing their names and values. It returns false as the second argument if the key could not be
// constructed, e.g. if one of the keys does not match a field in the row. keys must be sorted
// in ascending order.
func (r *Row) KeyValue(keys [][]byte) (uint64, bool) {
	return r.Data.KeyValue(keys)
}

// Reset clears the row of all data, preserving allocated memory.
func (r *Row) Reset() {
	r.ReceiveTime = time.Time{}
	r.DataTime = time.Time{}
	r.Data = r.Data[:0]
}

// FM is a field name with a list of measures
type FM struct {
	F []byte   // field name
	M [][]byte // list of measure names
}

// Query is a row of data to be collated containing fields and their values.
type Query struct {
	FieldMeasures []FM   // fields and the measures to take from them
	Criteria      FVList // fields and values in the row. Field names must be unique.
}

// KeyValue builds a key value by locating Query criteria fields corresponding
// to the key names supplied and hashing their names and values. It returns
// false as the second argument if the key could not be constructed, e.g. if
// one of the keys does not match a field in the query criteria. keys must be
// sorted in ascending order.
func (q *Query) KeyValue(keys [][]byte) (uint64, bool) {
	return q.Criteria.KeyValue(keys)
}

// Reset clears the query of all data, preserving allocated memory.
func (q *Query) Reset() {
	q.FieldMeasures = q.FieldMeasures[:0]
	q.Criteria = q.Criteria[:0]
}

// FMV is a field name with a measure name and a value
type FMV struct {
	F []byte // field name
	M []byte // measure name
	V interface{}
}

func (f FMV) String() string {
	return fmt.Sprintf("%s(%s)=%v", f.M, f.F, f.V)
}

// Result is a result of a query.
type Result struct {
	FieldMeasureValues []FMV // fields and the measure values
}

type Collator struct {
	name        string
	keys        [][]byte
	writers     []anyWriter
	contWriters map[string][]contWriter
	discWriters map[string][]discWriter
	contReaders map[string]map[string]contReader
	discReaders map[string]map[string]discReader
	size        int // Size of buffer required to write all measures, i.e. record size
}

// Update reads the row and updates buf according to the schema's measures.
// buf contains the existing state of the collation record and is updated in-
// place. buf must be long enough for the schema's data. If init is true then
// the buffer contains unitialised data and the measures should initialise
// their state. If an error is returned then the buffer may be in an
// inconsistent state and should not be used further.
func (c Collator) Update(r Row, buf []byte, init bool) error {
	// Update measures that operate on the row
	for _, w := range c.writers {
		if err := w.Update(buf, init); err != nil {
			return err
		}
	}

	for _, fv := range r.Data {
		// Update measures that operate on the field as a continuous value
		if ws, exists := c.contWriters[string(fv.F)]; exists {
			v, err := strconv.ParseFloat(string(fv.V), 64)
			if err != nil {
				return err
			}
			for _, w := range ws {
				if err := w.UpdateFloat64(buf, init, v); err != nil {
					return err
				}
			}
		}

		// Update measures that operate on the field as a discrete value
		if ws, exists := c.discWriters[string(fv.F)]; exists {
			for _, w := range ws {
				if err := w.UpdateItem(buf, init, fv.V); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Note that the byte slices in fml and buf must not be retained or reused by the collator.
func (c Collator) Read(fml []FM, buf []byte) (Result, error) {
	var res Result
	for _, fm := range fml {
		field := string(fm.F)

		if rs, exists := c.contReaders[field]; exists {
			for _, measure := range fm.M {
				if r, exists := rs[string(measure)]; exists {
					v, err := r.ReadFloat64(buf)
					if err != nil {
						return res, err
					}
					res.FieldMeasureValues = append(res.FieldMeasureValues, FMV{
						F: fm.F,
						M: measure,
						V: v,
					})
				}
			}
		}

		if rs, exists := c.discReaders[field]; exists {
			for _, measure := range fm.M {
				if r, exists := rs[string(measure)]; exists {
					v, err := r.ReadItem(buf)
					if err != nil {
						return res, err
					}
					res.FieldMeasureValues = append(res.FieldMeasureValues, FMV{
						F: fm.F,
						M: measure,
						V: v,
					})
				}
			}
		}
	}

	return res, nil
}

func (c Collator) Keys() [][]byte {
	return c.keys
}

func (c Collator) Size() int {
	return c.size
}

func (c Collator) Name() string {
	return c.name
}

type anyWriter struct {
	Low, High int // index range of data within buffer
	Fn        RowWriterFunc
}

func (a anyWriter) Update(buf []byte, init bool) error {
	return a.Fn(buf[a.Low:a.High], init)
}

type contWriter struct {
	Low, High int // index range of data within buffer
	Fn        ContWriterFunc
}

func (c contWriter) UpdateFloat64(buf []byte, init bool, v float64) error {
	return c.Fn(buf[c.Low:c.High], init, v)
}

type discWriter struct {
	Low, High int // index range of data within buffer
	Fn        DiscWriterFunc
}

func (d discWriter) UpdateItem(buf []byte, init bool, v []byte) error {
	return d.Fn(buf[d.Low:d.High], init, v)
}

type contReader struct {
	Low, High int // index range of data within buffer
	Fn        ContReaderFunc
}

func (c contReader) ReadFloat64(buf []byte) (float64, error) {
	return c.Fn(buf[c.Low:c.High])
}

type discReader struct {
	Low, High int // index range of data within buffer
	Fn        DiscReaderFunc
}

func (d discReader) ReadItem(buf []byte) ([]byte, error) {
	return d.Fn(buf[d.Low:d.High])
}
