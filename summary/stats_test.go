package summary

import (
	"testing"
)

func TestStats64(t *testing.T) {
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

	for i, tc := range testCases {
		buf := make([]byte, Stats64Size)
		s := NewStats64(buf)
		for _, o := range tc.obs {
			s.Update(o)
		}

		if s.Count() != tc.count {
			t.Errorf("case %d: got %v, wanted %v", i, s.Count(), tc.count)
		}
		if s.Sum() != tc.sum {
			t.Errorf("case %d: got %v, wanted %v", i, s.Sum(), tc.sum)
		}
		if s.Mean() != tc.mean {
			t.Errorf("case %d: got %v, wanted %v", i, s.Mean(), tc.mean)
		}
		if s.Variance() != tc.variance {
			t.Errorf("case %d: got %v, wanted %v", i, s.Variance(), tc.variance)
		}
	}
}
