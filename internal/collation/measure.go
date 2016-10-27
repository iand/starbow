package collation

import (
	"encoding/binary"
	"math"
	"time"
)

type ObsType int

const (
	Any        ObsType = 0 // Any indicates that the value of the observation is not used by a measure.
	Continuous ObsType = 1 // A Continuous observation is a number from a continuous set of values.
	Discrete   ObsType = 2 // A Discrete observation is a string or a number from a countable set of values.
)

// CoherentObsTypes reports whether all the supplied observation types are
// compatible with one another. All observation types are compatible with Any
// or themselves. Otherwise they are incompatible.
func CoherentObsTypes(os []ObsType) bool {
	primary := Any
	for _, o := range os {
		if o != primary {
			if primary != Any {
				return false
			}
			primary = o
		}
	}

	return true
}

type Measure interface {
	// ObsType returns the type of observation that the measure operates on.
	ObsType() ObsType
}

type Measures []Measure

// Coherent reports whether the measures have compatible observation types.
func (ms Measures) Coherent() bool {
	os := make([]ObsType, len(ms))
	for i := range ms {
		os[i] = ms[i].ObsType()
	}
	return CoherentObsTypes(os)
}

// Precise measures

// Count is a precise count of observations over all time.
type Count struct{}

func (Count) ObsType() ObsType { return Any }

func (Count) Size() int { return 8 }

func (Count) Update(buf []byte) error {
	c := binary.LittleEndian.Uint64(buf)
	c++
	binary.LittleEndian.PutUint64(buf, c)
	return nil
}

// Sum is a precise sum of observations over all time.
type Sum struct{}

func (Sum) ObsType() ObsType { return Continuous }

func (Sum) Size() int { return 8 }

func (Sum) UpdateFloat64(buf []byte, v float64) error {
	sum := math.Float64frombits(binary.LittleEndian.Uint64(buf))
	sum += v
	binary.LittleEndian.PutUint64(buf, math.Float64bits(sum))
	return nil
}

// Mean is a precise mean of observations over all time.
type Mean struct{}

func (Mean) ObsType() ObsType { return Continuous }

// Variance is a precise variance of observations over all time.
type Variance struct{}

func (Variance) ObsType() ObsType { return Continuous }

// Approximate measures

// LookbackCount is an approximate count of observations within a lookback window of time from the present.
type LookbackCount struct {
	Lookback time.Duration
}

func (LookbackCount) ObsType() ObsType { return Any }

// WindowedCount is an approximate count of observations within a fixed time window.
type WindowedCount struct {
	Interval Interval
}

func (WindowedCount) ObsType() ObsType { return Any }

// BucketedCount is an approximate count of observations within a series of equally sized time windows.
type BucketedCount struct {
	Buckets  int
	Duration time.Duration
}

func (BucketedCount) ObsType() ObsType { return Any }

// Cardinality is an approximate count of unique observations over all time.
type Cardinality struct {
}

func (Cardinality) ObsType() ObsType { return Discrete }

// LookbackCardinality is an approximate count of unique observations within a lookback window of time from the present.
type LookbackCardinality struct {
	Lookback time.Duration
}

func (LookbackCardinality) ObsType() ObsType { return Discrete }

// WindowedCardinality is an approximate count of unique observations within a fixed time window.
type WindowedCardinality struct {
	Interval Interval
}

func (WindowedCardinality) ObsType() ObsType { return Discrete }

type TopK struct {
	K int
}

func (TopK) ObsType() ObsType { return Discrete }

// Interval is an contiguous interval of time comprising all times after After and before Before.
type Interval struct {
	After  time.Time
	Before time.Time
}
