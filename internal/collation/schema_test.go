package collation

import (
	"testing"

	"github.com/iand/starbow/internal/hash/fnv"
)

func hash(vs ...string) uint64 {
	h := fnv.New64()
	for _, v := range vs {
		h.Write([]byte(v))
	}
	return h.Sum64()
}

func TestRowKeyValue(t *testing.T) {
	testCases := []struct {
		data  []FV
		keys  [][]byte
		hash  uint64
		found bool
	}{

		{
			data: []FV{
				{F: []byte("foo"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("foo")},
			hash:  hash("foo", "bar"),
			found: true,
		},

		{
			data: []FV{
				{F: []byte("fez"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("fez")},
			hash:  hash("fez", "bar"),
			found: true,
		},

		{
			data: []FV{
				{F: []byte("foo"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("fez")},
			hash:  0,
			found: false,
		},

		{
			data: []FV{
				{F: []byte("foo"), V: []byte("bar")},
				{F: []byte("fez"), V: []byte("bar")},
				{F: []byte("fuz"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("foo"), []byte("fuz")},
			hash:  hash("foo", "bar", "fuz", "bar"),
			found: true,
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			r := Row{
				Data: tc.data,
			}

			h, ok := r.KeyValue(tc.keys)

			if ok != tc.found {
				t.Fatalf("got found %v, wanted %v", ok, tc.found)
			}
			if !tc.found {
				return
			}

			if h != tc.hash {
				t.Fatalf("got hash %v, wanted %v", h, tc.hash)
			}
		})
	}
}

func TestQueryKeyValue(t *testing.T) {
	testCases := []struct {
		data  []FV
		keys  [][]byte
		hash  uint64
		found bool
	}{

		{
			data: []FV{
				{F: []byte("foo"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("foo")},
			hash:  hash("foo", "bar"),
			found: true,
		},

		{
			data: []FV{
				{F: []byte("fez"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("fez")},
			hash:  hash("fez", "bar"),
			found: true,
		},

		{
			data: []FV{
				{F: []byte("foo"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("fez")},
			hash:  0,
			found: false,
		},

		{
			data: []FV{
				{F: []byte("foo"), V: []byte("bar")},
				{F: []byte("fez"), V: []byte("bar")},
				{F: []byte("fuz"), V: []byte("bar")},
			},
			keys:  [][]byte{[]byte("foo"), []byte("fuz")},
			hash:  hash("foo", "bar", "fuz", "bar"),
			found: true,
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			q := Query{
				Criteria: tc.data,
			}

			h, ok := q.KeyValue(tc.keys)

			if ok != tc.found {
				t.Fatalf("got found %v, wanted %v", ok, tc.found)
			}
			if !tc.found {
				return
			}

			if h != tc.hash {
				t.Fatalf("got hash %v, wanted %v", h, tc.hash)
			}
		})
	}
}
