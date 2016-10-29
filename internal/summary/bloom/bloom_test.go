package bloom

import (
	"math/rand"
	"testing"

	"github.com/iand/starbow/internal/testutil"
)

func TestAdd(t *testing.T) {
	b := NewBits(1024, 6)
	b.Add([]byte("foo"))

	var nbits uint8
	// Count the bits set, should be equal to k
	for i := 0; i < b.bits.Count(); i++ {
		if b.bits.Get(i) == 1 {
			nbits++
		}
	}

	if nbits != b.k {
		t.Errorf("got %d, wanted %d", nbits, b.k)
	}
}

func TestHas(t *testing.T) {
	b := NewBits(1024, 6)
	b.Add([]byte("foo"))

	if !b.Has([]byte("foo")) {
		t.Errorf("got not found, wanted found")
	}

	if b.Has([]byte("bar")) {
		t.Errorf("got found, wanted not found")
	}
}

func BenchmarkAdd(b *testing.B) {
	testCases := []struct {
		name string
		n    int
		p    float64
		l    int // length of data values to be added
	}{

		{name: "small-short", n: 1000, p: 0.01, l: 8},
		{name: "small-med", n: 1000, p: 0.01, l: 80},
		{name: "small-long", n: 1000, p: 0.01, l: 800},
		{name: "small-xlong", n: 1000, p: 0.01, l: 8000},

		{name: "med-short", n: 10000, p: 0.01, l: 8},
		{name: "med-med", n: 10000, p: 0.01, l: 80},
		{name: "med-long", n: 10000, p: 0.01, l: 800},
		{name: "med-xlong", n: 10000, p: 0.01, l: 8000},

		{name: "large-short", n: 100000, p: 0.01, l: 8},
		{name: "large-med", n: 100000, p: 0.01, l: 80},
		{name: "large-long", n: 100000, p: 0.01, l: 800},
		{name: "large-xlong", n: 100000, p: 0.01, l: 8000},

		{name: "xlarge-short", n: 1000000, p: 0.01, l: 8},
		{name: "xlarge-med", n: 1000000, p: 0.01, l: 80},
		{name: "xlarge-long", n: 1000000, p: 0.01, l: 800},
		{name: "xlarge-xlong", n: 100000, p: 0.01, l: 8000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(1444))
			data := testutil.RandomByteSlices(500, tc.l, rng)

			bf := New(tc.n, tc.p)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bf.Add(data[i%500])
			}
		})
	}
}

func TestCount(t *testing.T) {
	bf := New(1000000, 0.01)
	rng := rand.New(rand.NewSource(232323))
	data := testutil.RandomByteSlices(50000, 8, rng)
	for i := range data {
		bf.Add(data[i])
	}
	t.Logf("countn: %v", bf.bits.CountN(1))
	count := bf.Count()
	if count < 49950 || count > 50050 {
		t.Errorf("got %d, wanted around 50000", count)
	}
}
