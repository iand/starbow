// Package seqlock implements a simple example of a seqlock which
// provides non-waiting reads on a data structure that is undergoing
// consurrent writes.
// See https://en.wikipedia.org/wiki/Seqlock
package seqlock

import (
	"sync"
	"sync/atomic"
)

type Seqlock struct {
	seq  uint32 // monotonically increasing write counter
	mu   sync.Mutex
	data [1024]byte
}

func New() *Seqlock {
	return &Seqlock{}
}

// Get returns the data in the seqlock
func (s *Seqlock) Get() (ret [1024]byte) {
	for {
		seq1 := atomic.LoadUint32(&s.seq)
		// Is a writer currently writing?
		if seq1%2 != 0 {
			continue
		}

		// Get a copy of the data
		copy(ret[:], s.data[:])

		// Check that the data was not updated during the copy
		seq2 := atomic.LoadUint32(&s.seq)

		if seq1 == seq2 {
			// No update was made
			return
		}
	}
}

// Put replaces the data in the seqlock
func (s *Seqlock) Put(v [1024]byte) {
	// Writers need to take a lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// Notify readers that we are writing
	atomic.AddUint32(&s.seq, 1)
	copy(s.data[:], v[:])

	// Notify readers that we are done
	atomic.AddUint32(&s.seq, 1)
}

// Update applies the update function to the data in the seqlock.
// If the function returns an error then the update is aborted.
func (s *Seqlock) Update(fn func(data []byte) error) {
	// Writers need to take a lock
	s.mu.Lock()

	// Notify readers that we are writing
	atomic.AddUint32(&s.seq, 1)

	var d [1024]byte
	copy(d[:], s.data[:])
	if err := fn(d[:]); err == nil {
		copy(s.data[:], d[:])
	}

	// Notify readers that we are done
	atomic.AddUint32(&s.seq, 1)
	s.mu.Unlock()
}
