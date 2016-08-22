// Package incmap implements a write-only lock free map
package incmap

import (
	"sync/atomic"
)

// Map is a write-only lock free map. Its implementation is
// simplified by not supplying a way of deleting keys. It is
// also a fixed size. Keys are assigned using linear probing.
type Map struct {
	l      int
	keys   []uint64
	values []uint64
}

// New creates a Map with the size l.
func New(l int) *Map {
	return &Map{
		l:      l,
		keys:   make([]uint64, l),
		values: make([]uint64, l),
	}
}

const maxReprobe = 10

// Inc increments the value stored against key k by amount d. It
// returns true if the increment succeeded, false otherwise, e.g.
// when the map is full.
func (m *Map) Inc(k uint64, d uint64) bool {
	// 0 is our value that indicates an empty slot so we can't accept
	// it as a valid key
	if k == 0 {
		return false
	}
	x := -1
	i := 0
	for {
		x = m.pos(k, i)

		// Is our key here?
		kexist := atomic.LoadUint64(&m.keys[x])
		if kexist == k {
			break
		}

		// Is it an empty slot?
		if kexist == 0 {
			// Try and store key into slot
			if atomic.CompareAndSwapUint64(&m.keys[x], 0, k) {
				// Stored key successfully
				break
			}
		}

		// Did another updater set the key while we were checking?
		kexist = atomic.LoadUint64(&m.keys[x])
		if kexist == k {
			break
		}

		i++
		if i > maxReprobe {
			// too many attempts to find an empty slot
			return false
		}
	}

	atomic.AddUint64(&m.values[x], d)
	return true
}

// Get retrieves the value stored against key k. The second return
// value will be false if the key could not be found in the map.
func (m *Map) Get(k uint64) (uint64, bool) {

	x := -1
	i := 0
	for {
		x = m.pos(k, i)
		if atomic.LoadUint64(&m.keys[x]) == k {
			break
		}

		i++
		if i > maxReprobe {
			// too many attempts to find slot
			return 0, false
		}
	}

	return atomic.LoadUint64(&m.values[x]), true
}

// pos maps the key and probe index to a slot location in the map.
// Uses a naive mod based algorithm but could use something more
// sophisticated like rendezvous hashing.
func (m *Map) pos(k uint64, i int) int {
	return int((k%uint64(m.l) + uint64(i)) % uint64(m.l))
}
