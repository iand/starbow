package collation

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/iand/starbow/internal/summary/hll"
	"github.com/iand/starbow/internal/summary/stat64"
)

type Measure interface {
	Size() int
	Name() string
}

type RowMeasure interface {
	Measure
	RowWriter() RowWriterFunc
}

type ContinuousInput interface {
	ContWriter() ContWriterFunc
}
type ContinuousOutput interface {
	ContReader() ContReaderFunc
}

type DiscreteInput interface {
	DiscWriter() DiscWriterFunc
}
type DiscreteOutput interface {
	DiscReader() DiscReaderFunc
}

type RowWriterFunc func(buf []byte, init bool) error
type ContWriterFunc func(buf []byte, init bool, v float64) error
type DiscWriterFunc func(buf []byte, init bool, v []byte) error

type ContReaderFunc func(buf []byte) (float64, error)
type DiscReaderFunc func(buf []byte) ([]byte, error)

// Precise measures

// Count is a precise count of observations over all time.
type Count struct{}

var _ RowMeasure = Count{}

func (Count) Size() int { return 8 }

func (Count) Name() string { return "count" }

func (Count) RowWriter() RowWriterFunc {
	return func(buf []byte, init bool) error {
		c := binary.LittleEndian.Uint64(buf)
		c++
		binary.LittleEndian.PutUint64(buf, c)
		return nil
	}
}

func (Count) ContReader() ContReaderFunc {
	return func(buf []byte) (float64, error) {
		c := binary.LittleEndian.Uint64(buf)
		return float64(c), nil
	}
}

// Sum is a precise sum of continuous observations over all time.
type Sum struct{}

var _ ContinuousInput = Sum{}

func (Sum) Size() int { return stat64.Len() }

func (Sum) Name() string { return "sum" }

func (Sum) ContWriter() ContWriterFunc {
	return func(buf []byte, init bool, v float64) error {
		s := stat64.WithBytes(buf)
		s.Update(v)
		return nil
	}
}

func (Sum) ContReader() ContReaderFunc {
	return func(buf []byte) (float64, error) {
		s := stat64.WithBytes(buf)
		return s.Sum(), nil
	}
}

// Mean is a precise mean of continuous observations over all time.
type Mean struct{}

var _ ContinuousInput = Mean{}

func (Mean) Size() int { return stat64.Len() }

func (Mean) Name() string { return "mean" }

func (Mean) ContWriter() ContWriterFunc {
	return func(buf []byte, init bool, v float64) error {
		s := stat64.WithBytes(buf)
		s.Update(v)
		return nil
	}
}

func (Mean) ContReader() ContReaderFunc {
	return func(buf []byte) (float64, error) {
		s := stat64.WithBytes(buf)
		return s.Mean(), nil
	}
}

// Variance is a precise variance of continuous observations over all time.
type Variance struct{}

var _ ContinuousInput = Variance{}

func (Variance) Size() int { return stat64.Len() }

func (Variance) Name() string { return "var" }

func (Variance) ContWriter() ContWriterFunc {
	return func(buf []byte, init bool, v float64) error {
		s := stat64.WithBytes(buf)
		s.Update(v)
		return nil
	}
}
func (Variance) ContReader() ContReaderFunc {
	return func(buf []byte) (float64, error) {
		s := stat64.WithBytes(buf)
		return s.Variance(), nil
	}
}

// Max is a precise maximum of continuous observations over all time.
type Max struct{}

var _ ContinuousInput = Max{}

func (Max) Size() int { return 8 }

func (Max) Name() string { return "max" }

func (Max) ContWriter() ContWriterFunc {
	return func(buf []byte, init bool, v float64) error {
		if !init {
			c := math.Float64frombits(binary.LittleEndian.Uint64(buf))
			if c >= v {
				return nil
			}
		}
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		return nil
	}
}

func (Max) ContReader() ContReaderFunc {
	return func(buf []byte) (float64, error) {
		c := math.Float64frombits(binary.LittleEndian.Uint64(buf))
		return c, nil
	}
}

// Min is a precise minimum of continuous observations over all time.
type Min struct{}

var _ ContinuousInput = Min{}

func (Min) Size() int { return 8 }

func (Min) Name() string { return "min" }

func (Min) ContWriter() ContWriterFunc {
	return func(buf []byte, init bool, v float64) error {
		if !init {
			c := math.Float64frombits(binary.LittleEndian.Uint64(buf))
			if c <= v {
				return nil
			}
		}
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		return nil
	}
}

func (Min) ContReader() ContReaderFunc {
	return func(buf []byte) (float64, error) {
		c := math.Float64frombits(binary.LittleEndian.Uint64(buf))
		return c, nil
	}
}

// Approximate measures

// // LookbackCount is an approximate count of observations within a lookback window of time from the present.
// type LookbackCount struct {
// 	Lookback time.Duration
// }

// func (LookbackCount) ObsType() ObsType { return Any }

// // WindowedCount is an approximate count of observations within a fixed time window.
// type WindowedCount struct {
// 	Interval Interval
// }

// func (WindowedCount) ObsType() ObsType { return Any }

// // BucketedCount is an approximate count of observations within a series of equally sized time windows.
// type BucketedCount struct {
// 	Buckets  int
// 	Duration time.Duration
// }

// func (BucketedCount) ObsType() ObsType { return Any }

// Cardinality is an approximate unique count of discrete observations over all time.
type Cardinality struct {
	Precision int // precision between 4 and 18, inclusive
}

var _ DiscreteInput = Cardinality{}

func (c Cardinality) DiscWriter() DiscWriterFunc {
	return func(buf []byte, init bool, v []byte) error {

		if init {
			s := hll.New(uint8(c.Precision))
			s.Add(v)

			var b bytes.Buffer
			_, err := s.WriteTo(&b) // TODO: ensure bytes.Buffer does not grow
			copy(buf, b.Bytes())
			return err
		}

		s, err := hll.WithBytes(buf)
		if err != nil {
			return err
		}
		s.Add(v)
		return nil
	}
}

func (c Cardinality) Size() int {
	return hll.Len(uint8(c.Precision))
}

func (Cardinality) Name() string { return "uniquecount" }

// // LookbackCardinality is an approximate count of unique observations within a lookback window of time from the present.
// type LookbackCardinality struct {
// 	Lookback time.Duration
// }

// func (LookbackCardinality) ObsType() ObsType { return Discrete }

// // WindowedCardinality is an approximate count of unique observations within a fixed time window.
// type WindowedCardinality struct {
// 	Interval Interval
// }

// func (WindowedCardinality) ObsType() ObsType { return Discrete }

// type TopK struct {
// 	K int
// }

// func (TopK) ObsType() ObsType { return Discrete }

// // Interval is an contiguous interval of time comprising all times after After and before Before.
// type Interval struct {
// 	After  time.Time
// 	Before time.Time
// }
