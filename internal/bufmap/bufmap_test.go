package bufmap

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"sync"
	"testing"
)

func TestEmpty(t *testing.T) {
	m := New(1000, 8)

	found := m.Get(100, []byte{})
	if found {
		t.Fatalf("got found, wanted not found")
	}

}

// Increment by 1
var inc = func(data []byte) error {
	val, _ := binary.ReadVarint(bytes.NewReader(data))
	binary.PutVarint(data, val+1)
	return nil
}

func TestMultiUpdate(t *testing.T) {
	n := 5
	runtime.GOMAXPROCS(n)

	m := New(1000, 8)

	l := 50000
	k := uint64(120)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(m *Map, i int, l int, k uint64) {
			for j := 0; j < l; j++ {
				m.Update(k, inc)
			}
			wg.Done()
		}(m, i, l, k)
	}

	wg.Wait()
	data := make([]byte, 8)
	ok := m.Get(k, data)
	if !ok {
		t.Fatalf("did not find key %d", k)
	}
	val, _ := binary.ReadVarint(bytes.NewReader(data[:]))

	if val != int64(n*l) {
		t.Errorf("got %d, wanted %d", val, n*l)
	}
}
