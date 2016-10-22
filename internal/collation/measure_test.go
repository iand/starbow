package collation

import (
	"testing"
)

func TestCoherentObsTypes(t *testing.T) {
	testCases := []struct {
		obs      []ObsType
		coherent bool
	}{
		{
			obs:      []ObsType{Any},
			coherent: true,
		},
		{
			obs:      []ObsType{Any, Any},
			coherent: true,
		},
		{
			obs:      []ObsType{Discrete},
			coherent: true,
		},
		{
			obs:      []ObsType{Discrete, Discrete},
			coherent: true,
		},
		{
			obs:      []ObsType{Any, Discrete},
			coherent: true,
		},
		{
			obs:      []ObsType{Continuous},
			coherent: true,
		},
		{
			obs:      []ObsType{Continuous, Continuous},
			coherent: true,
		},
		{
			obs:      []ObsType{Any, Continuous},
			coherent: true,
		},
		{
			obs:      []ObsType{Discrete, Continuous},
			coherent: false,
		},
		{
			obs:      []ObsType{Any, Discrete, Continuous},
			coherent: false,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := CoherentObsTypes(tc.obs)
			if actual != tc.coherent {
				t.Errorf("got %v, wanted %v", actual, tc.coherent)
			}
		})
	}
}

func TestMeasuresCoherent(t *testing.T) {
	testCases := []struct {
		ms       Measures
		coherent bool
	}{
		{
			ms:       Measures{Count{}}, // None
			coherent: true,
		},
		{
			ms:       Measures{Count{}, Sum{}}, // None + Continuous
			coherent: true,
		},
		{
			ms:       Measures{Mean{}, Sum{}}, // Continuous + Continuous
			coherent: true,
		},
		{
			ms:       Measures{Count{}, Cardinality{}}, // None + Discrete
			coherent: true,
		},
		{
			ms:       Measures{Cardinality{}, LookbackCardinality{}}, // Discrete + Discrete
			coherent: true,
		},
		{
			ms:       Measures{Mean{}, Cardinality{}}, // Continuous + Discrete
			coherent: false,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := tc.ms.Coherent()
			if actual != tc.coherent {
				t.Errorf("got %v, wanted %v", actual, tc.coherent)
			}
		})
	}
}
