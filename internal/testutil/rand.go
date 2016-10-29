package testutil

type Rand interface {
	// Intn returns a number in [0,n)
	Intn(n int) int
}

// RandomByteSlice creates a byte slice filled with n random bytes using rng as the source of randomness.
func RandomByteSlice(n int, rng Rand) []byte {
	buf := make([]byte, n)
	FillRandomByteSlice(buf, rng)
	return buf
}

// FillRandomByteSlice fills a byte slice with n random bytes using rng as the source of randomness.
func FillRandomByteSlice(buf []byte, rng Rand) {
	for i := range buf {
		buf[i] = Printable(rng)
	}
}

// Printable returns a byte from the range of printable ASCII characters (0x20-0x7E).
func Printable(rng Rand) byte {
	return ' ' + byte(rng.Intn(95))
}

// RandomByteSlices creates m byte slices each filled with n random bytes using rng as the source of randomness.
func RandomByteSlices(m, n int, rng Rand) [][]byte {
	bufs := make([][]byte, m)
	for i := range bufs {
		bufs[i] = make([]byte, n)
		FillRandomByteSlice(bufs[i], rng)
	}
	return bufs
}
