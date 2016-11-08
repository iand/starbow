package storage

import (
	"context"

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
	Update(k uint64, fn func(data []byte, init bool) error) error
}

type Store struct {
	Backend  Backend
	Collator collation.Collator
}

func (s *Store) Write(ctx context.Context, r collation.Row) error {
	key, found := r.KeyValue(s.Collator.Keys())
	if !found {
		// Nothing to do
		return nil
	}

	t := Transaction{
		Row: r,
		Fn:  s.Collator.Update,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return s.Backend.Update(key, t.Do)
}

func (s *Store) Read(ctx context.Context, q collation.Query) (collation.Result, error) {
	key, found := q.KeyValue(s.Collator.Keys())
	if !found {
		// Nothing to do
		return collation.Result{}, nil
	}

	// TODO: reuse buffer
	buf := make([]byte, s.Collator.Size())

	if !s.Backend.Get(key, buf) {
		// TODO: report not found?
		return collation.Result{}, nil
	}

	return s.Collator.Read(q.FieldMeasures, buf)
}

type Transaction struct {
	Row collation.Row
	Fn  func(r collation.Row, data []byte, init bool) error
}

func (t Transaction) Do(data []byte, init bool) error {
	return t.Fn(t.Row, data, init)
}
