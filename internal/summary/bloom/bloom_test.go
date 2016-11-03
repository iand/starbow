package bloom

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/iand/starbow/internal/bitbucket"
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

func TestWriteTo(t *testing.T) {
	var buf bytes.Buffer

	bf := NewBits(256, 3)
	n, err := bf.WriteTo(&buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bb := bitbucket.New(256, 1)

	expectedLen := int64(hdrLen + bb.Len())
	if n != expectedLen {
		t.Errorf("got %d bytes written, wanted %d", n, expectedLen)
	}

	// Get a serialized form of the equivalent bit bucket
	var bbBuf bytes.Buffer
	bb.WriteTo(&bbBuf)

	expected := append([]byte{Version, 3}, bbBuf.Bytes()...)
	actual := buf.Bytes()

	if !bytes.Equal(actual, expected) {
		t.Errorf("got %+v, wanted %+v", actual, expected)
	}
}

func serialize(bf Bloom) []byte {
	var buf bytes.Buffer
	bf.WriteTo(&buf)
	return buf.Bytes()
}

func TestReadFrom(t *testing.T) {
	bf := NewBits(256, 3)
	bf.Add([]byte("xyz"))

	data := serialize(bf)
	t.Logf("%+v", data)

	r := bytes.NewReader(data)

	var bf2 Bloom
	n, err := bf2.ReadFrom(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int(n) != len(data) {
		t.Errorf("got %d bytes read, wanted %d", n, len(data))
	}

	if bf2.k != 3 {
		t.Errorf("got %d hash functions, wanted %d", bf2.k, 3)
	}

	if bf2.m != 256 {
		t.Errorf("got %d bits, wanted %d", bf2.m, 256)
	}

	if !bf2.Has([]byte("xyz")) {
		t.Errorf("did not find xyz")
	}
}

func TestReadFromExtraData(t *testing.T) {
	bf := NewBits(256, 3)
	data := serialize(bf)
	data = append(data, 44) // extra trailing byte

	r := bytes.NewReader(data)
	_, err := bf.ReadFrom(r)
	if err != io.ErrShortBuffer {
		t.Fatalf("got %v error, wanted io.ErrShortBuffer", err)
	}
}

func TestReadFromChecksVersion(t *testing.T) {
	bf := NewBits(256, 3)
	data := serialize(bf)
	data[0] = Version + 1

	r := bytes.NewReader(data)
	_, err := bf.ReadFrom(r)
	if err != ErrIncompatibleVersion {
		t.Fatalf("got %v error, wanted ErrIncompatibleVersion", err)
	}
}

func TestWithBytes(t *testing.T) {
	bf := NewBits(256, 3)
	bf.Add([]byte("xyz"))

	data := serialize(bf)

	bf2, err := WithBytes(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if bf2.k != 3 {
		t.Errorf("got %d hash functions, wanted %d", bf2.k, 3)
	}

	if bf2.m != 256 {
		t.Errorf("got %d bits, wanted %d", bf2.m, 256)
	}

	if !bf2.Has([]byte("xyz")) {
		t.Errorf("did not find xyz")
	}
}

func TestWithBytesDoesNotAllocate(t *testing.T) {
	bf := NewBits(256, 3)
	bf.Add([]byte("xyz"))

	data := serialize(bf)
	allocs := testing.AllocsPerRun(1000, func() {
		_, err := WithBytes(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if allocs != 0 {
		t.Errorf("got %f allocations, wanted none", allocs)
	}
}
