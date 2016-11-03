package stats

import (
	"math"
)

// Meanvar calculates the mean and sample variance of vals.
func Meanvar(vals []float64) (mean, variance float64) {
	count := float64(len(vals))

	if count == 0 {
		return math.NaN(), math.NaN()
	} else if count == 1 {
		return vals[0], math.NaN()
	}

	sum := Sum(vals)

	mean = sum / count
	for i := range vals {
		variance += (vals[i] - mean) * (vals[i] - mean)
	}
	variance /= count
	return
}

// Sum calculates the sum of vals.
func Sum(vals []float64) float64 {
	sum := 0.0
	for i := range vals {
		sum += vals[i]
	}
	return sum
}

// See calculates the standard error of the estimate
func See(vals []float64, e float64) float64 {
	errsum := 0.0
	for _, v := range vals {
		errsum += (v - e) * (v - e)
	}
	return math.Sqrt(errsum / float64(len(vals)-2))
}
