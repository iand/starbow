package query

import (
	"reflect"
	"testing"

	"github.com/iand/starbow/internal/collation"
)

func TestRoughParse(t *testing.T) {
	testCases := []struct {
		input string
		query collation.Query
		err   bool
	}{

		{
			input: "select sum(price) where customer='jim'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim")},
				},
			},
		},

		{
			input: "select sum(price), mean(quantity) where customer='jim'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum")}},
					{F: []byte("quantity"), M: [][]byte{[]byte("mean")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim")},
				},
			},
		},

		{
			input: "select sum(price),mean(quantity) where customer='jim'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum")}},
					{F: []byte("quantity"), M: [][]byte{[]byte("mean")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim")},
				},
			},
		},

		{
			input: "select sum(price) where customer='jim' and country='UK'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim")},
					{F: []byte("country"), V: []byte("UK")},
				},
			},
		},

		{
			input: "select sum(price), mean(price) where customer='jim'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum"), []byte("mean")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim")},
				},
			},
		},

		{
			input: "SeLeCt sum(price) whERE customer='jim'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim")},
				},
			},
		},

		{
			input: "select sum(price) where customer='jim jones'",
			query: collation.Query{
				FieldMeasures: []collation.FM{
					{F: []byte("price"), M: [][]byte{[]byte("sum")}},
				},
				Criteria: []collation.FV{
					{F: []byte("customer"), V: []byte("jim jones")},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			q, err := RoughParse(tc.input)

			if err != nil && !tc.err {
				t.Fatalf("unexpected error: %v", err)
			}

			if err == nil && tc.err {
				t.Fatalf("expected error, got none")
			}

			if tc.err {
				return
			}

			if !reflect.DeepEqual(q, tc.query) {
				t.Errorf("got %+v, wanted %+v", q, tc.query)
			}
		})
	}
}
