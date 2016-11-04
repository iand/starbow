package storage

import (
	"github.com/iand/starbow/internal/collation"
)

type Backend interface {
	// Read a buffer value stored against key k into buffer buf.
	// The return value will be false if the key could not be found in the map.
	Get(k uint64, buf []byte) bool

	// Update finds the buffer stored against key k and atomically updates it with
	// v using the function fn. It returns the error returned by fn, if any. The
	// function fn may be called multiple times during a single call to Update, or
	// not called at all.
	Update(k uint64, fn func(data []byte) error) error
}

type Store struct {
	Backend  Backend
	Collator collation.Collator
}

func (s *Store) WriteRow(r collation.Row) error {
	key, found := r.KeyValue(s.Collator.Keys())
	if !found {
		// Nothing to do
		return nil
	}

	t := Transaction{
		Row: r,
		Fn:  s.Collator.Update,
	}

	return s.Backend.Update(key, t.Do)
}

type Transaction struct {
	Row collation.Row
	Fn  func(r collation.Row, data []byte, init bool) error
}

func (t Transaction) Do(data []byte) error {
	return t.Fn(t.Row, data, false) // TODO: pass init=true when it's a new item in the keyvalue store
}
