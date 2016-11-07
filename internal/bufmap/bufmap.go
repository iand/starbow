// Package bufmap implements a write-only multi-reader, single-writer map of
// byte buffers offering wait-free reads and consistent writes. The map consists
// of a fixed number of slots which are assigned to keys using linear probing. Reads
// and writes to the data held in a slot are guarded by a seqlock.
package bufmap

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidKey = errors.New("invalid key: 0")
	ErrOutOfSlots = errors.New("out of slots")
)

// Map is a write-only lock free map. Its implementation is
// simplified by not supplying a way of deleting keys. It is
// also a fixed size. Keys are assigned using linear probing.
type Map struct {
	l      int // length
	s      int // stride
	keys   []slot
	values []byte
}

// New creates a Map with l buffers each consisting of s bytes
func New(l, s int) *Map {
	return &Map{
		l:      l,
		s:      s,
		keys:   make([]slot, l),
		values: make([]byte, l*s),
	}
}

type slot struct {
	key uint64 // key occupying this slot
	seq uint32 // monotonically increasing write counter
	mu  sync.Mutex
}

// maximum number of hops to make during a linear probe.
const maxReprobe = 10

// Get atomically reads a buffer value stored against key k into buffer buf.
// The return value will be false if the key could not be found in the map.
func (m *Map) Get(k uint64, buf []byte) bool {
	// Locate the correct slot
	x := -1
	i := 0
	for {
		x = m.pos(k, i)
		if atomic.LoadUint64(&m.keys[x].key) == k {
			break
		}

		i++
		if i > maxReprobe {
			// too many attempts to find slot
			return false
		}
	}

	// Read the buffer from the slot using the seqlock
	s := &m.keys[x]
	for {
		seq1 := atomic.LoadUint32(&s.seq)
		// Is a writer currently writing?
		if seq1%2 != 0 {
			continue
		}

		// Get a copy of the data
		copy(buf, m.values[x*m.s:x*(m.s+1)])

		// Check that the data was not updated during the copy
		seq2 := atomic.LoadUint32(&s.seq)

		if seq1 == seq2 {
			// No update was made
			break
		}
	}

	return true
}

// Update finds the buffer stored against key k and atomically updates it with
// v using the function fn. It returns the error returned by fn, if any. It
// will also return an error if the map is full. The function fn may be called
// multiple times during a single call to Update, or not called at all if the
// map cannot find space for the key.
func (m *Map) Update(k uint64, fn func(data []byte, init bool) error) error {
	// 0 is our value that indicates an empty slot so we can't accept
	// it as a valid key
	if k == 0 {
		return ErrInvalidKey
	}
	x := -1
	i := 0
	init := false
	for {
		x = m.pos(k, i)

		// Is our key here?
		kexist := atomic.LoadUint64(&m.keys[x].key)
		if kexist == k {
			break
		}
		// Signal that the is a new value
		init = true

		// Is it an empty slot?
		if kexist == 0 {
			// Try and store key into slot
			if atomic.CompareAndSwapUint64(&m.keys[x].key, 0, k) {
				// Stored key successfully
				break
			}
		}

		// Did another writer set the key while we were checking?
		kexist = atomic.LoadUint64(&m.keys[x].key)
		if kexist == k {
			break
		}

		// Try next slot
		i++
		if i > maxReprobe {
			// too many attempts to find an empty slot
			return ErrOutOfSlots
		}
	}

	s := &m.keys[x]

	// Writers need to take a lock
	s.mu.Lock()

	// Notify readers that we are writing
	atomic.AddUint32(&s.seq, 1)

	err := fn(m.values[x*m.s:x*(m.s+1)], init)

	// Notify readers that we are done
	atomic.AddUint32(&s.seq, 1)

	// Allow access by other writers
	s.mu.Unlock()
	return err
}

// pos maps the key and probe index to a slot location in the map.
// Uses a naive mod based algorithm but could use something more
// sophisticated like rendezvous hashing.
func (m *Map) pos(k uint64, i int) int {
	return int((k%uint64(m.l) + uint64(i)) % uint64(m.l))
}
