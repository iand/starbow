package stat64

import (
	"testing"
)

func TestSummary(t *testing.T) {
	testCases := []struct {
		obs      []float64
		count    uint64
		sum      float64
		mean     float64
		variance float64
	}{

		{
			obs:      []float64{1, 4, 2, 6, 1, 7, 9, 3},
			count:    8,
			sum:      1 + 4 + 2 + 6 + 1 + 7 + 9 + 3,                                 // 33
			mean:     33.0 / 8.0,                                                    // 4.125
			variance: ((1.0 + 16 + 4 + 36 + 1 + 49 + 81 + 9) - (33.0*33)/8.0) / 7.0, // 8.696428571428571
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			s := New()
			for _, o := range tc.obs {
				s.Update(o)
			}

			if s.Count() != tc.count {
				t.Errorf("got %v, wanted %v", s.Count(), tc.count)
			}
			if s.Sum() != tc.sum {
				t.Errorf("got %v, wanted %v", s.Sum(), tc.sum)
			}
			if s.Mean() != tc.mean {
				t.Errorf("got %v, wanted %v", s.Mean(), tc.mean)
			}
			if s.Variance() != tc.variance {
				t.Errorf("got %v, wanted %v", s.Variance(), tc.variance)
			}
		})
	}
}

func TestSummaryUpdateMulti(t *testing.T) {
	testCases := []struct {
		obs      []float64
		count    uint64
		sum      float64
		mean     float64
		variance float64
	}{

		{
			obs:      []float64{1, 4, 2, 6, 1, 7, 9, 3},
			count:    8,
			sum:      1 + 4 + 2 + 6 + 1 + 7 + 9 + 3,                                 // 33
			mean:     33.0 / 8.0,                                                    // 4.125
			variance: ((1.0 + 16 + 4 + 36 + 1 + 49 + 81 + 9) - (33.0*33)/8.0) / 7.0, // 8.696428571428571
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			s := New()
			s.UpdateMulti(tc.obs)

			if s.Count() != tc.count {
				t.Errorf("got count %v, wanted %v", s.Count(), tc.count)
			}
			if s.Sum() != tc.sum {
				t.Errorf("got sum %v, wanted %v", s.Sum(), tc.sum)
			}
			if s.Mean() != tc.mean {
				t.Errorf("got mean %v, wanted %v", s.Mean(), tc.mean)
			}
			if s.Variance() != tc.variance {
				t.Errorf("got variance %v, wanted %v", s.Variance(), tc.variance)
			}
		})
	}
}

var benchres interface{}

func BenchmarkSummaryUpdate(b *testing.B) {
	samples := []float64{4, 7, 3, 1, 8, 1, 7, 9, 9, 0}
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Update(samples[i%10])
	}
	benchres = s.Mean()
}

func BenchmarkSummaryUpdateMulti(b *testing.B) {
	samples := []float64{4, 7, 3, 1, 8, 1, 7, 9, 9, 0}
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.UpdateMulti(samples)
	}
	benchres = s.Mean()
}

func BenchmarkReset(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Reset()
	}
	benchres = s.Mean()
}

func BenchmarkMean(b *testing.B) {
	s := New()
	for i := 1; i < 10; i++ {
		s.Update(float64(i))
	}
	b.ResetTimer()
	var v float64
	for i := 0; i < b.N; i++ {
		v = s.Mean()
	}
	benchres = v
}

func BenchmarkSum(b *testing.B) {
	s := New()
	for i := 1; i < 10; i++ {
		s.Update(float64(i))
	}
	b.ResetTimer()
	var v float64
	for i := 0; i < b.N; i++ {
		v = s.Sum()
	}
	benchres = v
}

func BenchmarkCount(b *testing.B) {
	s := New()
	for i := 1; i < 10; i++ {
		s.Update(float64(i))
	}
	b.ResetTimer()
	var v uint64
	for i := 0; i < b.N; i++ {
		v = s.Count()
	}
	benchres = v
}

func BenchmarkVariance(b *testing.B) {
	s := New()
	for i := 1; i < 10; i++ {
		s.Update(float64(i))
	}
	b.ResetTimer()
	var v float64
	for i := 0; i < b.N; i++ {
		v = s.Variance()
	}
	benchres = v
}
