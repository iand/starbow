package summary

import (
	"bytes"
	"encoding/binary"
	"math"
)

var zeroStats64 = bytes.Repeat([]byte{0}, 32)

const Stats64Size = 32

// Stats64 is a summary that maintains basic statistical measures for a series
// of observations. It requires 32 bytes.
type Stats64 struct {
	buf []byte
}

// NewStats64 creates a new Stats64 summary backed by the buffer buf which
// must be at least 32 bytes in length.
func NewStats64(buf []byte) Stats64 {
	return Stats64{buf: buf}
}

// Size returns the size of the summary's data, in bytes.
func (s Stats64) Size() int {
	return Stats64Size
}

// Update adds an observation to the stats summary.
func (s Stats64) Update(v float64) {
	count := binary.LittleEndian.Uint64(s.buf[:8])
	sum := math.Float64frombits(binary.LittleEndian.Uint64(s.buf[8:16]))
	mean := math.Float64frombits(binary.LittleEndian.Uint64(s.buf[16:24]))

	// ss is the sum of squares of differences from the current mean
	ss := math.Float64frombits(binary.LittleEndian.Uint64(s.buf[24:32]))

	count++
	sum += v

	// Calculatation based on section 4.2.2 of Knuth, Vol 2: Seminumerical Algorithms
	delta := v - mean
	mean += delta / float64(count)
	ss += delta * (v - mean)

	binary.LittleEndian.PutUint64(s.buf[:8], count)
	binary.LittleEndian.PutUint64(s.buf[8:16], math.Float64bits(sum))
	binary.LittleEndian.PutUint64(s.buf[16:24], math.Float64bits(mean))
	binary.LittleEndian.PutUint64(s.buf[24:32], math.Float64bits(ss))
}

// Reset returns all measures tracked by the summary to their zero values.
func (s Stats64) Reset() {
	copy(s.buf, zeroStats64)
}

// Count returns the number the series of observations.
func (s Stats64) Count() uint64 {
	return binary.LittleEndian.Uint64(s.buf[:8])
}

// Mean returns the mean of the observation values.
func (s Stats64) Mean() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(s.buf[16:24]))
}

// Variance returns the sample variance of the series of observations.
func (s Stats64) Variance() float64 {
	count := binary.LittleEndian.Uint64(s.buf[:8])
	if count < 2 {
		return math.NaN()
	}

	ss := math.Float64frombits(binary.LittleEndian.Uint64(s.buf[24:32]))
	return ss / float64(count-1)
}

// Sum returns the sum of the observation values.
func (s Stats64) Sum() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(s.buf[8:16]))
}
