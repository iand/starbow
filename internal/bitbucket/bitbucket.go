// Package bitbucket provides a data structure that comprises a collection of buckets with a configurable number of bits per bucket.

// Inspired by Buckets type in github.com/tylertreat/BoomFilters

package bitbucket

import (
	"encoding/binary"
	"io"
)

// A BitBucket is a collection of buckets capable of storing values from zero
// up to maximum specified by the configured number of bits.
type BitBucket struct {
	data []byte
	n    int
	w    uint8
}

// New creates a new BitBucket with n buckets each consisting of w bits.
func New(n int, w uint8) *BitBucket {
	return &BitBucket{
		n:    n,
		w:    w,
		data: make([]byte, length(n, w)),
	}
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
	var buf [9]byte
	buf[0] = b.w
	binary.LittleEndian.PutUint64(buf[1:9], uint64(b.n))

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
	var buf [9]byte

	n, err := io.ReadFull(r, buf[:])
	if err != nil {
		if err == io.EOF {
			return int64(n), io.ErrUnexpectedEOF
		}
		return int64(n), err
	}

	b.w = buf[0]
	b.n = int(binary.LittleEndian.Uint64(buf[1:9]))
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

// Reset resets the bit bucket to be empty, but it retains the underlying storage for use by future writes.
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
	return 1 + 8 + len(b.data)
}
