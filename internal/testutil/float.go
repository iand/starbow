package testutil

import (
	"math"
)

// Close64 reports whether two 64-bit floats are equal within a given tolerance.
func Close64(a, b float64, tolerance float64) bool {
	return AdjacentEqual64(a, b) || math.Abs(a-b)/a <= tolerance
}

// Equiv64 reports whether two 64-bit floats are equivalent. It returns true if
// they are both infinite, NaN or numerically equal to one another.
func Equiv64(a, b float64) bool {
	if math.IsInf(a, 0) && math.IsInf(b, 0) {
		return true
	}
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return a == b
}

// EquivOrCloseFloat64 reports whether two 64-bit floats are equivalent. It returns true if
// they are both infinite, NaN or numerically equal within a given tolerance.
func EquivOrCloseFloat64(a, b float64, tolerance float64) bool {
	if math.IsInf(a, 0) && math.IsInf(b, 0) {
		return true
	}
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return Close64(a, b, tolerance)
}

// AdjacentEqual64 reports whether a equals b or they are adjacent floating point numbers.
func AdjacentEqual64(a, b float64) bool {
	return a == b || a == math.Nextafter(b, math.MaxFloat64) || b == math.Nextafter(a, math.MaxFloat64)
}
