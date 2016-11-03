// Package bloom provides a bloom filter.
package bloom

import (
	"errors"
	"io"
	"math"

	"github.com/iand/starbow/internal/bitbucket"
	"github.com/iand/starbow/internal/hash/fnv"
)

const (
	Version = 1 // Serialization version numnber
	hdrLen  = 2 // number of bytes needed for header data when serializing (version + number of hash functions)
)

var ErrIncompatibleVersion = errors.New("bloom: incompatible version")

// Bloom is a bloom filter which can be used to probabilisticly test whether an item is a
// member of a set. False positives are possible but false negatives are not.
// Items may be added but cannot be removed from the set.
type Bloom struct {
	bits *bitbucket.BitBucket
	m    int   // number of bits in bloom filter
	k    uint8 // number of hash functions
}

// New creates a new bloom filter suitable for storing n items with a false positive rate of p.
func New(n int, p float64) *Bloom {
	m, k := mk(n, p)
	return NewBits(m, k)
}

func mk(n int, p float64) (int, uint8) {
	m := int(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	kraw := math.Ceil(math.Ln2 * float64(m) / float64(n))
	var k uint8 = 255
	if kraw < 255 {
		k = uint8(kraw)
	}

	return m, k
}

// NewBits creates a new bloom filter with m bits and k hash functions.
func NewBits(m int, k uint8) *Bloom {
	return &Bloom{
		m:    m,
		k:    k,
		bits: bitbucket.New(m, 1),
	}
}

// Add adds v to the bloom filter.
func (b *Bloom) Add(v []byte) {
	h := newHasher(v, b.m)
	for i := uint8(0); i < b.k; i++ {
		x := int(h.next())
		b.bits.Set(x, 1)
	}
}

// Has reports whether v is possibly present in the bloom filter. If false
// then the item is definitely not in the set.
func (b *Bloom) Has(v []byte) bool {
	h := newHasher(v, b.m)
	for i := uint8(0); i < b.k; i++ {
		if b.bits.Get(int(h.next())) != 1 {
			return false
		}
	}
	return true
}

// Len returns the length of the buffer required to serialize the bloom filter.
func (b *Bloom) Len() int {
	return hdrLen + b.bits.Len()
}

// Len returns the length of the buffer required to serialize a bloom filter
// suitable for storing n items with a false positive rate of p.
func Len(n int, p float64) int {
	m, _ := mk(n, p)
	return hdrLen + bitbucket.Len(m, 1)
}

// Count returns an estimate of the number of items that have been added to the bloom filter.
func (b *Bloom) Count() int64 {
	x := b.bits.CountN(1)
	return int64(math.Ceil(-math.Log(1-float64(x)/float64(b.m)) * float64(b.m) / float64(b.k)))
}

// ErrorRate returns an estimate of the rate of false positives returned by the bloom filter's Has method.
func (b *Bloom) ErrorRate() float64 {
	x := b.bits.CountN(1)
	return math.Pow(1-math.Exp(-float64(b.k)*float64(x)/float64(b.m)), float64(b.k))
}

// WriteTo writes a binary representation of the bloom filter to w. It adheres
// to the io.WriterTo interface protocol. The return value is the number
// of bytes written. Any error encountered during the write is also returned.
func (b *Bloom) WriteTo(w io.Writer) (int64, error) {
	var buf [hdrLen]byte
	buf[0] = Version
	buf[1] = b.k

	n, err := w.Write(buf[:])
	if err != nil {
		return int64(n), err
	}

	n0, err := b.bits.WriteTo(w)
	n += int(n0)
	if err != nil {
		return int64(n), err
	}

	return int64(n), nil
}

// ReadFrom reads a binary representation of the bloom filter from r overwriting
// any previous configuration. It adheres to the io.ReaderFrom interface
// protocol. It reads data from r until EOF or error. The return value n is the
// number of bytes read. Any error except io.EOF encountered during the read
// is also returned.
func (b *Bloom) ReadFrom(r io.Reader) (int64, error) {
	var buf [hdrLen]byte

	n, err := io.ReadFull(r, buf[:])
	if err != nil {
		if err == io.EOF {
			return int64(n), io.ErrUnexpectedEOF
		}
		return int64(n), err
	}

	version := buf[0]
	if version != Version {
		return int64(n), ErrIncompatibleVersion
	}

	b.k = buf[1]

	if b.bits == nil {
		b.bits = &bitbucket.BitBucket{}
	}

	n0, err := b.bits.ReadFrom(r)
	n += int(n0)
	if err != nil {
		if err == io.EOF {
			return int64(n), io.ErrUnexpectedEOF
		}
		return int64(n), err
	}
	b.m = b.bits.Count()

	n1, err := r.Read(buf[:])
	n += n1
	if err != io.EOF {
		// Unexpected trailing data
		return int64(n), io.ErrShortBuffer
	}

	return int64(n), nil
}

// Reset resets the bloom filter to be empty retaining the underlying allocated storage for use by future writes.
func (b *Bloom) Reset() {
	b.bits.Reset()
}

// hasher provides double hashing as per Dillinger, Peter C.; Manolios,
// Panagiotis (2004b), "Bloom Filters in Probabilistic Verification",
// Proceedings of the 5th International Conference on Formal Methods in
// Computer-Aided Design, Springer-Verlag, Lecture Notes in Computer Science
// 3312
type hasher struct {
	a, b uint64
	m    int
	n    int
}

// newHasher creates a hasher that hashes data two different ways and uses the
// resulting values as seeds for a sequence of hash values. m is the maximum
// allowable hash value to return from the next method.
func newHasher(data []byte, m int) hasher {
	h1 := fnv.New64()
	h1.Write(data)
	a := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write(data)
	b := h2.Sum64()

	// b should be non-zero and relatively prime to m
	if m&1 == 0 {
		// m is even, so ensure b is odd
		if b&1 == 0 {
			b++
		}
	} else {
		// m is not even; make it so
		if m > 2 {
			m--
		}
		// Ensure b is not even too
		if b&1 == 0 {
			b++
		}
	}

	return hasher{
		a: a,
		b: b,
		m: m,
	}
}

// next returns the next hash in the sequence
func (h *hasher) next() uint64 {
	v := h.a % uint64(h.m)
	h.a = (h.a + h.b) % uint64(h.m)
	return v
}
