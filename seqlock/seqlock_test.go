package seqlock

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"sync"
	"testing"
)

func TestPutThenGet(t *testing.T) {
	testdata := []byte("test")

	s := New()

	data := [1024]byte{}
	copy(data[:], testdata)
	s.Put(data)

	x := s.Get()
	if !bytes.Equal(x[:4], testdata) {
		t.Errorf("got %v, wanted %v", x[:4], testdata)
	}
}

func TestMultiUpdate(t *testing.T) {
	n := 5
	runtime.GOMAXPROCS(n)

	// Increment by 1
	inc := func(data []byte) error {
		val, _ := binary.ReadVarint(bytes.NewReader(data))
		binary.PutVarint(data, val+1)
		return nil
	}

	s := New()
	l := 50000

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(s *Seqlock, l int) {
			for j := 0; j < l; j++ {
				s.Update(inc)
			}
			wg.Done()
		}(s, l)
	}

	wg.Wait()
	data := s.Get()
	val, _ := binary.ReadVarint(bytes.NewReader(data[:]))

	if val != int64(n*l) {
		t.Errorf("got %d, wanted %d", val, n*l)
	}
}
