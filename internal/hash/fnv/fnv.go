// Package fnv implements FNV-1 and FNV-1a, non-cryptographic hash functions with non-allocating
// convenience methods for hashing of common types. Compatible with standard library hash/fnv.
package fnv

type (
	// 64-bit FNV-1 hash.
	Fnv64 uint64

	// 64-bit FNV-1a hash.
	Fnv64a uint64
)

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// New64 returns a new 64-bit FNV-1 hash.
func New64() *Fnv64 {
	var s Fnv64 = offset64
	return &s
}

// Reset resets the Hash to its initial state.
func (s *Fnv64) Reset() {
	*s = offset64
}

// Size returns the number of bytes Sum will return.
func (s *Fnv64) Size() int {
	return 8
}

// Sum64 returns the current hash value.
func (s *Fnv64) Sum64() uint64 {
	return uint64(*s)
}

// BlockSize returns the hash's underlying block size.
func (s *Fnv64) BlockSize() int {
	return 1
}

// Sum appends the current hash to buf in big-endian byte order and returns the resulting slice.
func (s *Fnv64) Sum(buf []byte) []byte {
	return sum(uint64(*s), buf)
}

// Write adds the bytes in data to the running hash. It never returns an error.
func (s *Fnv64) Write(data []byte) (int, error) {
	hash := *s // avoid dereferencing s during loop because it significantly changes benchmark speed
	for _, c := range data {
		hash *= prime64
		hash ^= Fnv64(c)
	}
	*s = hash
	return len(data), nil
}

// Write adds the single byte b to the running hash. It never returns an error.
func (s *Fnv64) WriteByte(b byte) error {
	*s *= prime64
	*s ^= Fnv64(b)
	return nil
}

// WriteString adds the bytes of string str to the running hash and returns the number of bytes written.
// It never returns an error.
func (s *Fnv64) WriteString(str string) (int, error) {
	hash := *s
	for i := 0; i < len(str); i++ {
		hash *= prime64
		hash ^= Fnv64(str[i])
	}
	*s = hash
	return len(str), nil
}

// WriteUint64 adds the 8 bytes of n to the running hash in big-endian byte order.
func (s *Fnv64) WriteUint64(n uint64) {
	hash := *s

	hash *= prime64
	hash ^= Fnv64(n >> 56)

	hash *= prime64
	hash ^= Fnv64((n >> 48) & 0xff)

	hash *= prime64
	hash ^= Fnv64((n >> 40) & 0xff)

	hash *= prime64
	hash ^= Fnv64((n >> 32) & 0xff)

	hash *= prime64
	hash ^= Fnv64((n >> 24) & 0xff)

	hash *= prime64
	hash ^= Fnv64((n >> 16) & 0xff)

	hash *= prime64
	hash ^= Fnv64((n >> 8) & 0xff)

	hash *= prime64
	hash ^= Fnv64(n & 0xff)

	*s = hash
}

// New64a returns a new 64-bit FNV-1a hash.
func New64a() *Fnv64a {
	var s Fnv64a = offset64
	return &s
}

// Reset resets the Hash to its initial state.
func (s *Fnv64a) Reset() {
	*s = offset64
}

// Size returns the number of bytes Sum will return.
func (s *Fnv64a) Size() int {
	return 8
}

// Sum64 returns the current hash value.
func (s *Fnv64a) Sum64() uint64 {
	return uint64(*s)
}

// BlockSize returns the hash's underlying block size.
func (s *Fnv64a) BlockSize() int {
	return 1
}

// Sum appends the current hash to buf in big-endian byte order and returns the resulting slice.
func (s *Fnv64a) Sum(buf []byte) []byte {
	return sum(uint64(*s), buf)
}

// Write adds the bytes in data to the running hash. It never returns an error.
func (s *Fnv64a) Write(data []byte) (int, error) {
	hash := *s
	for _, c := range data {
		hash ^= Fnv64a(c)
		hash *= prime64
	}
	*s = hash
	return len(data), nil
}

// Write adds the single byte b to the running hash. It never returns an error.
func (s *Fnv64a) WriteByte(b byte) error {
	*s ^= Fnv64a(b)
	*s *= prime64
	return nil
}

// WriteString adds the bytes of string str to the running hash and returns the number of bytes written.
// It never returns an error.
func (s *Fnv64a) WriteString(str string) (int, error) {
	hash := *s
	for i := 0; i < len(str); i++ {
		hash ^= Fnv64a(str[i])
		hash *= prime64
	}
	*s = hash
	return len(str), nil
}

// WriteUint64 adds the 8 bytes of n to the running hash in big-endian byte order.
func (s *Fnv64a) WriteUint64(n uint64) {
	hash := *s

	hash ^= Fnv64a(n >> 56)
	hash *= prime64

	hash ^= Fnv64a((n >> 48) & 0xff)
	hash *= prime64

	hash ^= Fnv64a((n >> 40) & 0xff)
	hash *= prime64

	hash ^= Fnv64a((n >> 32) & 0xff)
	hash *= prime64

	hash ^= Fnv64a((n >> 24) & 0xff)
	hash *= prime64

	hash ^= Fnv64a((n >> 16) & 0xff)
	hash *= prime64

	hash ^= Fnv64a((n >> 8) & 0xff)
	hash *= prime64

	hash ^= Fnv64a(n & 0xff)
	hash *= prime64

	*s = hash
}

func sum(n uint64, buf []byte) []byte {
	return append(buf, byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32), byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

// String64 returns a 64-bit hash of str using the FNV-1 algorithm.
func String64(str string) uint64 {
	hash := uint64(offset64)
	for i := 0; i < len(str); i++ {
		hash *= prime64
		hash ^= uint64(str[i])
	}
	return uint64(hash)
}

// String64a returns a 64-bit hash of str using the FNV-1a algorithm.
func String64a(str string) uint64 {
	hash := uint64(offset64)
	for i := 0; i < len(str); i++ {
		hash ^= uint64(str[i])
		hash *= prime64
	}
	return uint64(hash)
}

// Sum64 returns a 64-bit hash of buf using the FNV-1 algorithm.
func Sum64(buf []byte) uint64 {
	hash := uint64(offset64)
	for _, b := range buf {
		hash *= prime64
		hash ^= uint64(b)
	}
	return uint64(hash)
}

// Sum64a returns a 64-bit hash of buf using the FNV-1a algorithm.
func Sum64a(buf []byte) uint64 {
	hash := uint64(offset64)
	for _, b := range buf {
		hash ^= uint64(b)
		hash *= prime64
	}
	return uint64(hash)
}
