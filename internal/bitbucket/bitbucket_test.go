package bitbucket

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

var (
	// 4 buckets of 8 bits each
	// Values: 13,22,36,199
	bb4x8 = BitBucket{
		data: []byte{13, 22, 36, 199},
		n:    4,
		w:    8,
	}

	// 4 buckets of 4 bits each
	// Values: 13,10,1,8
	bb4x4 = BitBucket{
		data: []byte{0xDA, 0x18},
		n:    4,
		w:    4,
	}

	// 4 buckets of 3 bits each
	// Values: 7,2,1,6
	bb4x3 = BitBucket{
		data: []byte{0xE8, 0xE0},
		n:    4,
		w:    3,
	}
)

func TestLength(t *testing.T) {
	testCases := []struct {
		n      int
		w      uint8
		length int
	}{
		{n: 1, w: 8, length: 1},
		{n: 60, w: 8, length: 60},
		{n: 1, w: 4, length: 1},
		{n: 2, w: 4, length: 1},
		{n: 13, w: 4, length: 7},
		{n: 2, w: 3, length: 1},
		{n: 3, w: 3, length: 2},
		{n: 8, w: 3, length: 3},
		{n: 65, w: 1, length: 9},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := length(tc.n, tc.w)
			if actual != tc.length {
				t.Errorf("got %d, wanted %d", actual, tc.length)
			}
		})
	}
}

func TestGet(t *testing.T) {
	testCases := []struct {
		b BitBucket
		n int
		v uint8
	}{

		{
			b: bb4x8,
			n: 0,
			v: 13,
		},

		{
			b: bb4x8,
			n: 3,
			v: 199,
		},

		{
			b: bb4x4,
			n: 0,
			v: 13,
		},

		{
			b: bb4x4,
			n: 3,
			v: 8,
		},

		{
			b: bb4x3,
			n: 0,
			v: 7,
		},
		{
			b: bb4x3,
			n: 1,
			v: 2,
		},
		{
			b: bb4x3,
			n: 2,
			v: 1,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			actual := tc.b.Get(tc.n)
			if actual != tc.v {
				t.Errorf("got %d, wanted %d", actual, tc.v)
			}
		})
	}

}

func TestSet(t *testing.T) {
	testCases := []struct {
		b    BitBucket
		n    int
		v    uint8
		data []byte
	}{

		0: {
			b:    bb4x8,
			n:    0,
			v:    12,
			data: []byte{12, 22, 36, 199},
		},

		1: {
			b:    bb4x8,
			n:    3,
			v:    20,
			data: []byte{13, 22, 36, 20},
		},

		2: {
			b:    bb4x4,
			n:    0,
			v:    3,
			data: []byte{0x3A, 0x18},
		},

		3: {
			b:    bb4x4,
			n:    3,
			v:    0,
			data: []byte{0xDA, 0x10},
		},

		4: {
			b:    bb4x3,
			n:    1,
			v:    7,
			data: []byte{0xFC, 0xE0},
		},
		5: {
			b:    bb4x3,
			n:    2,
			v:    3,
			data: []byte{0xE9, 0xE0},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			bb := BitBucket{
				data: make([]byte, len(tc.b.data)),
				n:    tc.b.n,
				w:    tc.b.w,
			}
			copy(bb.data, tc.b.data)

			bb.Set(tc.n, tc.v)

			if !bytes.Equal(bb.data, tc.data) {
				t.Errorf("got %04x, wanted %04x", bb.data, tc.data)
			}
		})
	}

}

func TestIncN(t *testing.T) {
	testCases := []struct {
		b    BitBucket
		n    int
		v    uint8
		data []byte
	}{

		0: {
			b:    bb4x8,
			n:    0,
			v:    3,
			data: []byte{16, 22, 36, 199},
		},

		1: {
			b:    bb4x8,
			n:    3,
			v:    100,
			data: []byte{13, 22, 36, 255},
		},

		2: {
			b:    bb4x4,
			n:    0,
			v:    1,
			data: []byte{0xEA, 0x18}, // 14,10,1,8
		},

		3: {
			b:    bb4x4,
			n:    0,
			v:    15,
			data: []byte{0xFA, 0x18}, // 15,10,1,8
		},

		4: {
			b:    bb4x4,
			n:    3,
			v:    1,
			data: []byte{0xDA, 0x19}, // 13,10,1,9
		},

		5: {
			b:    bb4x3,
			n:    1,
			v:    1,
			data: []byte{0xEC, 0xE0}, // 7,3,1,6
		},

		6: {
			b:    bb4x3,
			n:    2,
			v:    3,
			data: []byte{0xEA, 0x60}, // 7,2,4,6
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			bb := BitBucket{
				data: make([]byte, len(tc.b.data)),
				n:    tc.b.n,
				w:    tc.b.w,
			}
			copy(bb.data, tc.b.data)

			bb.IncN(tc.n, tc.v)

			if !bytes.Equal(bb.data, tc.data) {
				t.Errorf("got %04x, wanted %04x", bb.data, tc.data)
			}
		})
	}

}

func TestWriteTo(t *testing.T) {
	var buf bytes.Buffer

	n, err := bb4x8.WriteTo(&buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n != (1 + 1 + 8 + 4) {
		t.Errorf("got %d bytes written, wanted %d", n, 1+1+8+4)
	}

	expected := []byte{Version, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199}
	actual := buf.Bytes()

	if !bytes.Equal(actual, expected) {
		t.Errorf("got %+v, wanted %+v", actual, expected)
	}
}

func TestReadFrom(t *testing.T) {
	var bb BitBucket
	r := bytes.NewReader([]byte{Version, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199})
	n, err := bb.ReadFrom(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n != (1 + 1 + 8 + 4) {
		t.Errorf("got %d bytes read, wanted %d", n, 1+1+8+4)
	}

	if bb.w != 8 {
		t.Errorf("got %d width, wanted %d", bb.w, 8)
	}

	if bb.n != 4 {
		t.Errorf("got %d buckets, wanted %d", bb.n, 4)
	}

	expected := []byte{13, 22, 36, 199}

	if !bytes.Equal(bb.data, expected) {
		t.Errorf("got %+v, wanted %+v", bb.data, expected)
	}
}

func TestReadFromExtraData(t *testing.T) {
	var bb BitBucket
	r := bytes.NewReader([]byte{Version, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199, 44}) // extra trailing byte
	_, err := bb.ReadFrom(r)
	if err != io.ErrShortBuffer {
		t.Fatalf("got %v error, wanted io.ErrShortBuffer", err)
	}
}

func TestReadFromChecksVersion(t *testing.T) {
	var bb BitBucket
	r := bytes.NewReader([]byte{Version + 1, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199})
	_, err := bb.ReadFrom(r)
	if err != ErrIncompatibleVersion {
		t.Fatalf("got %v error, wanted ErrIncompatibleVersion", err)
	}
}

func TestWithBytes(t *testing.T) {
	data := []byte{Version, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199}

	bb, err := WithBytes(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if bb.w != 8 {
		t.Errorf("got %d width, wanted %d", bb.w, 8)
	}

	if bb.n != 4 {
		t.Errorf("got %d buckets, wanted %d", bb.n, 4)
	}

	expected := []byte{13, 22, 36, 199}

	if !bytes.Equal(bb.data, expected) {
		t.Errorf("got %+v, wanted %+v", bb.data, expected)
	}
}

func TestWithBytesAdoptsBuffer(t *testing.T) {
	data := []byte{Version, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199}

	bb, err := WithBytes(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bb.Set(3, 20)
	if data[13] != 20 {
		t.Logf("data: %+v", data)
		t.Errorf("got %v, wanted 20", data[13])
	}
}

func TestWithBytesDoesNotAllocate(t *testing.T) {
	data := []byte{Version, 8, 4, 0, 0, 0, 0, 0, 0, 0, 13, 22, 36, 199}
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

var res interface{}

func BenchmarkGet(b *testing.B) {
	for w := 1; w < 9; w++ {
		b.Run(fmt.Sprintf("width%d", w), func(b *testing.B) {
			bb := New(100, uint8(w))
			for i := 0; i < 100; i++ {
				bb.Set(i, uint8(i%8))
			}
			b.ResetTimer()
			b.ReportAllocs()
			var x uint8
			for i := 0; i < b.N; i++ {
				x = bb.Get(i % 100)
			}
			res = x
		})
	}
}

func BenchmarkSet(b *testing.B) {
	for w := 1; w < 9; w++ {
		b.Run(fmt.Sprintf("width%d", w), func(b *testing.B) {
			bb := New(100, uint8(w))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bb.Set(i%100, uint8(i%8))
			}
		})
	}
}
