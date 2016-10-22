package fnv

import (
	stdfnv "hash/fnv"
	"testing"
)

const testStr = "We are such stuff as dreams are made on, and our little life, is rounded with a sleep."

var h uint64

func BenchmarkFnv64Write(b *testing.B) {
	data := []byte(testStr)
	hasher := New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write(data)
		h = hasher.Sum64()
	}
}

func BenchmarkFnv64aWrite(b *testing.B) {
	data := []byte(testStr)
	hasher := New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write(data)
		h = hasher.Sum64()
	}
}

func TestFnv64WriteByte(t *testing.T) {
	ehasher := stdfnv.New64()
	ehasher.Write([]byte{0x54, 0x32, 0x21, 0x87, 0x70})
	expected := ehasher.Sum64()

	ahasher := New64()
	ahasher.WriteByte(0x54)
	ahasher.WriteByte(0x32)
	ahasher.WriteByte(0x21)
	ahasher.WriteByte(0x87)
	ahasher.WriteByte(0x70)
	actual := ahasher.Sum64()

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkFnv64WriteByte(b *testing.B) {
	hasher := New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.WriteByte(0x54)
		h = hasher.Sum64()
	}
}

func TestFnv64aWriteByte(t *testing.T) {
	ehasher := stdfnv.New64a()
	ehasher.Write([]byte{0x54, 0x32, 0x21, 0x87, 0x70})
	expected := ehasher.Sum64()

	ahasher := New64a()
	ahasher.WriteByte(0x54)
	ahasher.WriteByte(0x32)
	ahasher.WriteByte(0x21)
	ahasher.WriteByte(0x87)
	ahasher.WriteByte(0x70)
	actual := ahasher.Sum64()

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkFnv64aWriteByte(b *testing.B) {
	hasher := New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.WriteByte(0x54)
		h = hasher.Sum64()
	}
}

func TestFnv64WriteString(t *testing.T) {
	ehasher := stdfnv.New64()
	ehasher.Write([]byte(testStr))
	expected := ehasher.Sum64()

	ahasher := New64()
	ahasher.WriteString(testStr)
	actual := ahasher.Sum64()

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkFnv64WriteString(b *testing.B) {
	hasher := New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.WriteString(testStr)
		h = hasher.Sum64()
	}
}

func TestFnv64aWriteString(t *testing.T) {
	ehasher := stdfnv.New64a()
	ehasher.Write([]byte(testStr))
	expected := ehasher.Sum64()

	ahasher := New64a()
	ahasher.WriteString(testStr)
	actual := ahasher.Sum64()

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkFnv64aWriteString(b *testing.B) {
	hasher := New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.WriteString(testStr)
		h = hasher.Sum64()
	}
}

func TestFnv64WriteUint64(t *testing.T) {
	n := uint64(443245628119291119)

	ehasher := stdfnv.New64()
	ehasher.Write([]byte{byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32), byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
	expected := ehasher.Sum64()

	ahasher := New64()
	ahasher.WriteUint64(n)
	actual := ahasher.Sum64()

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkFnv64WriteUint64(b *testing.B) {
	n := uint64(443245628119291119)
	hasher := New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.WriteUint64(n)
		h = hasher.Sum64()
	}
}

func TestFnv64aWriteUint64(t *testing.T) {
	n := uint64(443245628119291119)

	ehasher := stdfnv.New64a()
	ehasher.Write([]byte{byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32), byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
	expected := ehasher.Sum64()

	ahasher := New64a()
	ahasher.WriteUint64(n)
	actual := ahasher.Sum64()

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkFnv64aWriteUint64(b *testing.B) {
	n := uint64(443245628119291119)
	hasher := New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.WriteUint64(n)
		h = hasher.Sum64()
	}
}

func TestString64(t *testing.T) {
	ehasher := stdfnv.New64()
	ehasher.Write([]byte(testStr))
	expected := ehasher.Sum64()

	actual := String64(testStr)

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkString64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h = String64(testStr)
	}
}

func TestString64a(t *testing.T) {
	ehasher := stdfnv.New64a()
	ehasher.Write([]byte(testStr))
	expected := ehasher.Sum64()

	actual := String64a(testStr)

	if actual != expected {
		t.Errorf("got %v, wanted %v", actual, expected)
	}
}

func BenchmarkString64a(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h = String64a(testStr)
	}
}

func BenchmarkStdFnv64Write(b *testing.B) {
	data := []byte(testStr)
	hasher := stdfnv.New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write(data)
		h = hasher.Sum64()
	}
}

func BenchmarkStdFnv64aWrite(b *testing.B) {
	data := []byte(testStr)
	hasher := stdfnv.New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write(data)
		h = hasher.Sum64()
	}
}

func BenchmarkStdFnv64WriteByte(b *testing.B) {
	hasher := stdfnv.New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write([]byte{0x54})
		h = hasher.Sum64()
	}
}
func BenchmarkStdFnv64aWriteByte(b *testing.B) {
	hasher := stdfnv.New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write([]byte{0x54})
		h = hasher.Sum64()
	}
}

func BenchmarkStdFnv64WriteString(b *testing.B) {
	hasher := stdfnv.New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write([]byte(testStr))
		h = hasher.Sum64()
	}
}

func BenchmarkStdFnv64aWriteString(b *testing.B) {
	hasher := stdfnv.New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write([]byte(testStr))
		h = hasher.Sum64()
	}
}
func BenchmarkStdFnv64WriteUint64(b *testing.B) {
	n := uint64(443245628119291119)
	hasher := stdfnv.New64()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write([]byte{byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32), byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
		h = hasher.Sum64()
	}
}

func BenchmarkStdFnv64aWriteUint64(b *testing.B) {
	n := uint64(443245628119291119)
	hasher := stdfnv.New64a()
	for i := 0; i < b.N; i++ {
		hasher.Reset()
		hasher.Write([]byte{byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32), byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
		h = hasher.Sum64()
	}
}
