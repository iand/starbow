// Package stat64 provides a byte buffer-based statistical summary that
// maintains the mean, count, variance and sum of float64 samples.
package stat64

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Obs is a single observation that is intended to be included in the summary.
type Obs float64
type ObsList []float64

// Update adds the observation to the summary represented by the supplied byte buffer.
func (o Obs) Update(buf []byte) error {
	Summary(buf).Update(o)
	return nil
}

// Update adds the list of observations to the summary represented by the supplied byte buffer.
func (os ObsList) UpdateMulti(buf []byte) error {
	Summary(buf).UpdateMulti(os)
	return nil
}

var zeroStats64 = bytes.Repeat([]byte{0}, Size)

// Reset returns all measures tracked by the summary represented by the supplied byte buffer to their zero values.
func Reset(buf []byte) error {
	copy(buf, zeroStats64)
	return nil
}

// Size is the size of a Summary in bytes
const Size = 32

// Summary is a summary that maintains basic statistical measures for a series
// of observations. It requires 32 bytes.
type Summary []byte

// New creates a new stat64 Summary backed by the buffer buf which
// must be at least 32 bytes in length.
func New(buf []byte) Summary {
	return Summary(buf)
}

// Size returns the size of the summary's data, in bytes.
func (s Summary) Size() int {
	return Size
}

// Count returns the number the series of observations.
func (s Summary) Count() uint64 {
	return binary.LittleEndian.Uint64(s[:8])
}

// Mean returns the mean of the observation values.
func (s Summary) Mean() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(s[16:24]))
}

// Variance returns the sample variance of the series of observations.
func (s Summary) Variance() float64 {
	count := binary.LittleEndian.Uint64(s[:8])
	if count < 2 {
		return math.NaN()
	}

	ss := math.Float64frombits(binary.LittleEndian.Uint64(s[24:32]))
	return ss / float64(count-1)
}

// Sum returns the sum of the observation values.
func (s Summary) Sum() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(s[8:16]))
}

// Update adds an observation to the summary.
func (s Summary) Update(o Obs) {
	count := binary.LittleEndian.Uint64(s[:8])
	sum := math.Float64frombits(binary.LittleEndian.Uint64(s[8:16]))
	mean := math.Float64frombits(binary.LittleEndian.Uint64(s[16:24]))

	// ss is the sum of squares of differences from the current mean
	ss := math.Float64frombits(binary.LittleEndian.Uint64(s[24:32]))

	count++
	sum += float64(o)

	// Calculatation based on section 4.2.2 of Knuth, Vol 2: Seminumerical Algorithms
	delta := float64(o) - mean
	mean += delta / float64(count)
	ss += delta * (float64(o) - mean)

	binary.LittleEndian.PutUint64(s[:8], count)
	binary.LittleEndian.PutUint64(s[8:16], math.Float64bits(sum))
	binary.LittleEndian.PutUint64(s[16:24], math.Float64bits(mean))
	binary.LittleEndian.PutUint64(s[24:32], math.Float64bits(ss))
}

// UpdateMulti adds a list of observation to the summary.
func (s Summary) UpdateMulti(os ObsList) {
	count := binary.LittleEndian.Uint64(s[:8])
	sum := math.Float64frombits(binary.LittleEndian.Uint64(s[8:16]))
	mean := math.Float64frombits(binary.LittleEndian.Uint64(s[16:24]))

	// ss is the sum of squares of differences from the current mean
	ss := math.Float64frombits(binary.LittleEndian.Uint64(s[24:32]))

	for _, o := range os {
		count++
		sum += float64(o)

		// Calculatation based on section 4.2.2 of Knuth, Vol 2: Seminumerical Algorithms
		delta := float64(o) - mean
		mean += delta / float64(count)
		ss += delta * (float64(o) - mean)
	}

	binary.LittleEndian.PutUint64(s[:8], count)
	binary.LittleEndian.PutUint64(s[8:16], math.Float64bits(sum))
	binary.LittleEndian.PutUint64(s[16:24], math.Float64bits(mean))
	binary.LittleEndian.PutUint64(s[24:32], math.Float64bits(ss))
}

// Reset returns all measures tracked by the summary to their zero values.
func (s Summary) Reset() error {
	copy(s, zeroStats64)
	return nil
}
