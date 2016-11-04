package collation

import (
	"bytes"
	"encoding/binary"

	"github.com/iand/starbow/internal/summary/hll"
	"github.com/iand/starbow/internal/summary/stat64"
)

type Measure interface {
	Size() int
}

type RowMeasure interface {
	Measure
	RowWriter() RowWriterFunc
}

type ContinuousMeasure interface {
	Measure
	ContWriter() ContWriterFunc
}

type DiscreteMeasure interface {
	Measure
	DiscWriter() DiscWriterFunc
	DiscNew() DiscWriterFunc
}

type RowWriterFunc func(buf []byte) error
type ContWriterFunc func(buf []byte, v float64) error
type DiscWriterFunc func(buf []byte, v []byte) error

// Precise measures

// Count is a precise count of observations over all time.
type Count struct{}

func (Count) Size() int { return 8 }

func (Count) RowWriter() RowWriterFunc {
	return func(buf []byte) error {
		c := binary.LittleEndian.Uint64(buf)
		c++
		binary.LittleEndian.PutUint64(buf, c)
		return nil
	}
}

// Sum is a precise sum of continuous observations over all time.
type Sum struct{}

func (Sum) Size() int { return stat64.Len() }

func (Sum) ContWriter() ContWriterFunc {
	return func(buf []byte, v float64) error {
		s := stat64.WithBytes(buf)
		s.Update(stat64.Obs(v))
		return nil
	}
}

// Mean is a precise mean of continuous observations over all time.
type Mean struct{}

func (Mean) Size() int { return stat64.Len() }

func (Mean) ContWriter() ContWriterFunc {
	return func(buf []byte, v float64) error {
		s := stat64.WithBytes(buf)
		s.Update(stat64.Obs(v))
		return nil
	}
}

// Variance is a precise variance of continuous observations over all time.
type Variance struct{}

func (Variance) Size() int { return stat64.Len() }

func (Variance) ContWriter() ContWriterFunc {
	return func(buf []byte, v float64) error {
		s := stat64.WithBytes(buf)
		s.Update(stat64.Obs(v))
		return nil
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

func (c Cardinality) DiscUpdater() DiscWriterFunc {
	return func(buf []byte, v []byte) error {
		s, err := hll.WithBytes(buf)
		if err != nil {
			return err
		}
		s.Add(v)
		return nil
	}
}

func (c Cardinality) DiscNew() DiscWriterFunc {
	return func(buf []byte, v []byte) error {
		s := hll.New(uint8(c.Precision))
		s.Add(v)

		b := bytes.NewBuffer(buf)
		_, err := s.WriteTo(b) // TODO: ensure bytes.Buffer does not grow
		return err
	}
}

func (c Cardinality) Size() int {
	return hll.Len(uint8(c.Precision))
}

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
