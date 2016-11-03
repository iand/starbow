// +build ignore

// Dump error estimates for various HLL algorithms
// Run with go run errest.go

package main

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/iand/starbow/internal/summary/hll"
	"github.com/iand/starbow/internal/testutil"
)

func main() {
	seeds := []int64{200676831, 179907565, 77385072, 180153802, 136374130, 59043157, 1870872, 160189103, 106978570, 45344704}

	for p := uint8(4); p < 19; p++ {
		datasets := make([][][]byte, len(seeds))
		max := int(uint(1) << uint(p+5))
		d := 1
		for {
			for i, seed := range seeds {
				rng := rand.New(rand.NewSource(seed))
				datasets[i] = testutil.RandomByteSlices(d, 8, rng)
			}

			serr, serralt, serrlinear := estimateErrors(p, datasets)

			winner := "linear"

			if serr < serrlinear {
				winner = "harmonic"
			}

			if serralt < serr && serralt < serrlinear {
				winner = "poisson"
			}

			fmt.Printf("p: % 3d; d: % 8d; %s (h: %f, p: %f, l: %f)\n", p, d, winner, serr, serralt, serrlinear)

			d = (d * 3) / 2
			if d == 1 {
				d = 5
			}
			if d > max {
				break
			}

		}
	}
}

func estimateErrors(p uint8, datasets [][][]byte) (float64, float64, float64) {

	errsum := 0.0
	errsumalt := 0.0
	errsumlinear := 0.0
	for _, data := range datasets {
		c := hll.New(p)
		for i := range data {
			c.Add(data[i])
		}
		count := c.HarmonicCount()
		errsum += (float64(len(data)) - float64(count)) * (float64(len(data)) - float64(count))

		countalt := c.PoissonCount()
		errsumalt += (float64(len(data)) - float64(countalt)) * (float64(len(data)) - float64(countalt))

		countlinear := c.LinearCount()
		errsumlinear += (float64(len(data)) - float64(countlinear)) * (float64(len(data)) - float64(countlinear))

	}

	n := len(datasets)
	return stderr(errsum, n), stderr(errsumalt, n), stderr(errsumlinear, n)
}

func stderr(sum float64, n int) float64 {
	return math.Sqrt(sum / float64(n))
}
