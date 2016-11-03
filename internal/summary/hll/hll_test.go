package hll

import (
	"math/rand"
	"testing"

	"github.com/iand/starbow/internal/stats"
	"github.com/iand/starbow/internal/testutil"
)

func TestCount(t *testing.T) {

	seeds := []int64{256307119, 126465191, 239994928, 359761297, 279461460, 107961527, 192002531, 224757666, 338052841, 324311747}

	testCases := []struct {
		p uint8   // precision
		d int64   // number of distinct items
		e float64 // maximum standard error
	}{
		{p: 4, d: 4, e: 1.1},
		{p: 4, d: 8, e: 2.5},
		{p: 4, d: 16, e: 4.0},
		{p: 4, d: 40, e: 9.0},
		{p: 4, d: 400, e: 100.0},
		{p: 4, d: 4000, e: 1200.0},

		{p: 10, d: 40, e: 1.2},
		{p: 10, d: 400, e: 10.0},
		{p: 10, d: 4000, e: 120.0},
		{p: 10, d: 40000, e: 1200.0},

		{p: 14, d: 40, e: 1.2},
		{p: 14, d: 400, e: 2.6},
		{p: 14, d: 4000, e: 20.0},
		{p: 14, d: 40000, e: 400.0},
		{p: 14, d: 400000, e: 3500.0},

		{p: 16, d: 10, e: 1.2},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			counts := make([]float64, len(seeds))
			for i, seed := range seeds {
				c := New(tc.p)

				rng := rand.New(rand.NewSource(seed))
				data := testutil.RandomByteSlices(int(tc.d), 8, rng)

				for i := range data {
					c.Add(data[i])
				}

				counts[i] = float64(c.Count())
			}

			see := stats.See(counts, float64(tc.d))
			if see > tc.e {
				t.Logf("precision: %d, cardinality: %d", tc.p, tc.d)
				t.Logf("counts: %+v", counts)
				t.Errorf("got %f, wanted less than %f", see, tc.e)
			}

		})
	}
}

var res interface{}

func BenchmarkEstimators(b *testing.B) {

	linearFn := func(c *Counter) func() int64 { return c.LinearCount }
	poissonFn := func(c *Counter) func() int64 { return c.PoissonCount }
	harmonicFn := func(c *Counter) func() int64 { return c.HarmonicCount }

	testCases := []struct {
		name string
		fn   func(c *Counter) func() int64 // function to pick counter type function
		p    uint8                         // precision
		d    int                           // number of distinct items
	}{

		{name: "linear-low-small", fn: linearFn, p: 6, d: 100},
		{name: "linear-low-med", fn: linearFn, p: 6, d: 5000},
		{name: "linear-low-large", fn: linearFn, p: 6, d: 50000},
		{name: "linear-low-xlarge", fn: linearFn, p: 6, d: 500000},

		{name: "linear-med-small", fn: linearFn, p: 10, d: 100},
		{name: "linear-med-med", fn: linearFn, p: 10, d: 5000},
		{name: "linear-med-large", fn: linearFn, p: 10, d: 50000},
		{name: "linear-med-xlarge", fn: linearFn, p: 10, d: 500000},

		{name: "linear-high-small", fn: linearFn, p: 14, d: 100},
		{name: "linear-high-med", fn: linearFn, p: 14, d: 5000},
		{name: "linear-high-large", fn: linearFn, p: 14, d: 50000},
		{name: "linear-high-xlarge", fn: linearFn, p: 14, d: 500000},

		{name: "poisson-low-small", fn: poissonFn, p: 6, d: 100},
		{name: "poisson-low-med", fn: poissonFn, p: 6, d: 5000},
		{name: "poisson-low-large", fn: poissonFn, p: 6, d: 50000},
		{name: "poisson-low-xlarge", fn: poissonFn, p: 6, d: 500000},

		{name: "poisson-med-small", fn: poissonFn, p: 10, d: 100},
		{name: "poisson-med-med", fn: poissonFn, p: 10, d: 5000},
		{name: "poisson-med-large", fn: poissonFn, p: 10, d: 50000},
		{name: "poisson-med-xlarge", fn: poissonFn, p: 10, d: 500000},
		{name: "poisson-med-xxlarge", fn: poissonFn, p: 10, d: 5000000},

		{name: "poisson-high-small", fn: poissonFn, p: 14, d: 100},
		{name: "poisson-high-med", fn: poissonFn, p: 14, d: 5000},
		{name: "poisson-high-large", fn: poissonFn, p: 14, d: 50000},
		{name: "poisson-high-xlarge", fn: poissonFn, p: 14, d: 500000},
		{name: "poisson-high-xxlarge", fn: poissonFn, p: 14, d: 5000000},

		{name: "harmonic-low-small", fn: harmonicFn, p: 6, d: 100},
		{name: "harmonic-low-med", fn: harmonicFn, p: 6, d: 5000},
		{name: "harmonic-low-large", fn: harmonicFn, p: 6, d: 50000},
		{name: "harmonic-low-xlarge", fn: harmonicFn, p: 6, d: 500000},

		{name: "harmonic-med-small", fn: harmonicFn, p: 10, d: 100},
		{name: "harmonic-med-med", fn: harmonicFn, p: 10, d: 5000},
		{name: "harmonic-med-large", fn: harmonicFn, p: 10, d: 50000},
		{name: "harmonic-med-xlarge", fn: harmonicFn, p: 10, d: 500000},
		{name: "harmonic-med-xxlarge", fn: harmonicFn, p: 10, d: 5500000},

		{name: "harmonic-high-small", fn: harmonicFn, p: 14, d: 100},
		{name: "harmonic-high-med", fn: harmonicFn, p: 14, d: 5000},
		{name: "harmonic-high-large", fn: harmonicFn, p: 14, d: 50000},
		{name: "harmonic-high-xlarge", fn: harmonicFn, p: 14, d: 500000},
		{name: "harmonic-high-xxlarge", fn: harmonicFn, p: 14, d: 5000000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(1444))
			data := testutil.RandomByteSlices(tc.d, 8, rng)

			c := New(tc.p)
			for i := range data {
				c.Add(data[i])
			}
			fn := tc.fn(c)

			b.ReportAllocs()
			b.ResetTimer()
			var x int64
			for i := 0; i < b.N; i++ {
				x = fn()
			}
			res = x
		})
	}
}
