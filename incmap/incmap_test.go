package incmap

import (
	"runtime"
	"sync"
	"testing"
)

func TestEmpty(t *testing.T) {
	m := New(1000)

	_, found := m.Get(100)
	if found {
		t.Fatalf("got found, wanted not found")
	}

}

func TestInc(t *testing.T) {
	m := New(1000)

	if !m.Inc(100, 1) {
		t.Fatalf("failed to increment by 1")
	}

	actual, found := m.Get(100)
	if !found {
		t.Fatalf("got not found, wanted found")
	}

	if actual != 1 {
		t.Errorf("got %d, wanted 1", actual)
	}

	if !m.Inc(100, 2) {
		t.Fatalf("failed to increment by 2")
	}

	actual, found = m.Get(100)
	if !found {
		t.Fatalf("got not found, wanted found")
	}

	if actual != 3 {
		t.Errorf("got %d, wanted 3", actual)
	}
}

func TestIncMultiple(t *testing.T) {
	m := New(1000)

	for i := 1; i < 50; i++ {
		if !m.Inc(uint64(i), uint64(i)) {
			t.Fatalf("failed to increment %d by %d", i, i)
		}
	}
	for i := 1; i < 50; i++ {
		if !m.Inc(uint64(i), uint64(2*i)) {
			t.Fatalf("failed to increment %d by 2*%d", i, i)
		}
	}

	for i := 1; i < 50; i++ {
		actual, found := m.Get(uint64(i))
		if !found {
			t.Fatalf("got not found, wanted found")
		}

		if actual != uint64(i*3) {
			t.Errorf("got %d, wanted %d", actual, i*3)
		}

	}
}

func TestIncConcurrent2(t *testing.T) {
	doTestIncConcurrent(2, t)
}

func TestIncConcurrent10(t *testing.T) {
	doTestIncConcurrent(10, t)
}
func TestIncConcurrent100(t *testing.T) {
	doTestIncConcurrent(100, t)
}

func doTestIncConcurrent(n int, t *testing.T) {
	runtime.GOMAXPROCS(n)
	m := New(1000)
	s := 50000
	key := uint64(100)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(m *Map, k uint64) {
			for j := 0; j < s; j++ {
				m.Inc(k, 1)
			}
			wg.Done()
		}(m, key)
	}

	wg.Wait()
	actual, found := m.Get(key)
	if !found {
		t.Fatalf("got not found, wanted found")
	}
	if actual != uint64(n*s) {
		t.Errorf("got %d, wanted %d", actual, uint64(n*s))
	}
}
