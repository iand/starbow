// Package bitbucket provides a data structure that comprises a collection of buckets with a configurable number of bits per bucket.

// Inspired by Buckets type in github.com/tylertreat/BoomFilters

package bitbucket

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	Version = 1
	hdrLen  = 10 // number of bytes needed for header data when serializing (version, n and w)
)

var ErrIncompatibleVersion = errors.New("bitbucket: incompatible version")
var ErrUnsupportedWidth = errors.New("bitbucket: requested bucket width exceeds maximum of 8")

// A BitBucket is a collection of buckets capable of storing values from zero
// up to maximum specified by the configured number of bits.
type BitBucket struct {
	data []byte
	n    int
	w    uint8
}

// New creates a new BitBucket with n buckets each consisting of w bits. When w is 1 then the
// bitbucket is equivalent to a bitset. Panics if w > 8.
func New(n int, w uint8) BitBucket {
	if w > 8 {
		panic(ErrUnsupportedWidth)
	}
	return BitBucket{
		n:    n,
		w:    w,
		data: make([]byte, length(n, w)),
	}
}

// WithBytes creates a new bit bucket that uses buf as its backing storage,
// preserving any existing data in the byte slice. Any subsequent writes to
// the bit bucket will mutate buf. The layout of the byte buffer must match
// the layout used by the WriteTo method on an equivalent bit bucket.
func WithBytes(buf []byte) (BitBucket, error) {
	if len(buf) < hdrLen {
		return BitBucket{}, io.ErrShortBuffer
	}

	version := buf[0]
	if version != Version {
		return BitBucket{}, ErrIncompatibleVersion
	}

	w := buf[1]
	if w > 8 {
		return BitBucket{}, ErrUnsupportedWidth
	}

	n := int(binary.LittleEndian.Uint64(buf[2:hdrLen]))

	buflen := length(n, w)
	if len(buf) < hdrLen+buflen {
		return BitBucket{}, io.ErrShortBuffer
	}

	return BitBucket{
		n:    n,
		w:    w,
		data: buf[hdrLen : hdrLen+buflen],
	}, nil
}

// length calculates the length of byte slice needed to accommodate n buckets of w bits
func length(n int, w uint8) int {
	return (n*int(w) + 7) / 8
}

// Get returns the value stored in bucket i
func (b *BitBucket) Get(i int) uint8 {
	return b.get(i*int(b.w), b.w)
}

// Set sets the value stored in bucket i to v, capping it by the maximum value allowed
// for the bucket size.
func (b *BitBucket) Set(i int, v uint8) {
	b.set(i*int(b.w), b.w, v)
}

// IncN increments the value stored in bucket i by v, capping the result by
// the maximum value allowed for the bucket size.
func (b *BitBucket) IncN(i int, v uint8) {
	max := uint8(1<<b.w - 1)
	if v > max {
		v = max
	}
	b.incn(i*int(b.w), b.w, v)
}

// get returns the value of the l bits stored at offset o
func (b *BitBucket) get(o int, l uint8) uint8 {
	data, shift, mask, _ := b.extract(o, l)
	return uint8(uint16((data & mask) >> shift))
}

func (b *BitBucket) set(o int, l uint8, v uint8) {
	max := uint8(1<<b.w - 1)
	if v > max {
		v = max
	}

	data, shift, mask, index := b.extract(o, l)
	data = data&^mask | uint16(v)<<shift

	b.data[index] = uint8((data & 0xff00) >> 8)
	if index < len(b.data)-1 {
		b.data[index+1] = uint8(data & 0x00ff)
	}
}

func (b *BitBucket) incn(o int, l uint8, v uint8) {
	data, shift, mask, index := b.extract(o, l)
	value := uint16((data & mask) >> shift)
	max := uint16(1<<b.w - 1)

	// Cap the value
	value += uint16(v)
	if value > max {
		value = max
	}
	data = data&^mask | value<<shift

	b.data[index] = uint8((data & 0xff00) >> 8)
	if index < len(b.data)-1 {
		b.data[index+1] = uint8(data & 0x00ff)
	}
}

func (b *BitBucket) extract(o int, l uint8) (data uint16, shift uint16, mask uint16, index int) {
	index = o / 8          // which byte in the slice
	offset := uint8(o % 8) // which bit in the byte

	data = uint16(b.data[index]) << 8
	if index < len(b.data)-1 {
		data |= uint16(b.data[index+1])
	}

	shift = uint16(16 - offset - l)
	mask = uint16((1<<l)-1) << shift

	return
}

// WriteTo writes a binary representation of the bit bucket to w. It adheres
// to the io.WriterTo interface protocol. The return value is the number
// of bytes written. Any error encountered during the write is also returned.
func (b *BitBucket) WriteTo(w io.Writer) (int64, error) {
	var buf [hdrLen]byte
	buf[0] = Version
	buf[1] = b.w
	binary.LittleEndian.PutUint64(buf[2:hdrLen], uint64(b.n))

	n, err := w.Write(buf[:])
	if err != nil {
		return int64(n), err
	}

	n0, err := w.Write(b.data)
	n += n0
	if err != nil {
		return int64(n), err
	}

	return int64(n), nil
}

// ReadFrom reads a binary representation of the bit bucket from r overwriting
// any previous configuration. It adheres to the io.ReaderFrom interface
// protocol. It reads data from r until EOF or error. The return value n is the
// number of bytes read. Any error except io.EOF encountered during the read
// is also returned.
func (b *BitBucket) ReadFrom(r io.Reader) (int64, error) {
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

	b.w = buf[1]
	b.n = int(binary.LittleEndian.Uint64(buf[2:hdrLen]))
	b.data = make([]byte, length(b.n, b.w))

	n0, err := io.ReadFull(r, b.data)
	n += n0
	if err != nil {
		if err == io.EOF {
			return int64(n), io.ErrUnexpectedEOF
		}
		return int64(n), err
	}

	n1, err := r.Read(buf[:])
	n += n1
	if err != io.EOF {
		// Unexpected trailing data
		return int64(n), io.ErrShortBuffer
	}

	return int64(n), nil
}

// Reset zeroes all buckets in the bit bucket without reallocating the backing buffer.
func (b *BitBucket) Reset() {
	for i := range b.data {
		b.data[i] = 0
	}
}

// Max returns the maximum value that can be stored in a bucket.
func (b *BitBucket) Max() uint8 {
	return (1<<b.w - 1)
}

// Len returns the length of the buffer required to serialize the bit bucket.
func (b *BitBucket) Len() int {
	return 1 + 1 + 8 + len(b.data)
}

// Count returns the number of buckets.
func (b *BitBucket) Count() int {
	return b.n
}

// CountN returns the number of buckets that have the value v.
// TODO: optimize CountN by iterating through raw byte slice
func (b *BitBucket) CountN(v uint8) int {
	n := 0
	for i := 0; i < b.n; i++ {
		if b.Get(i) == v {
			n++
		}
	}
	return n
}

// Width returns the bit width of the buckets.
func (b *BitBucket) Width() uint8 {
	return b.w
}

// Len returns the length of the buffer required to serialize a bit bucket with n buckets each of width w bits.
func Len(n int, w uint8) int {
	return 1 + 1 + 8 + length(n, w)
}
