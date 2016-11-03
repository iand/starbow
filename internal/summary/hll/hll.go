// Package hll provides a hyperloglog structure for estimating the cardinality of multisets
package hll

import (
	"errors"
	"fmt"
	"math"

	"github.com/iand/starbow/internal/bitbucket"
	"github.com/iand/starbow/internal/hash/fnv"
)

var _ = fmt.Printf

const (
	// Version is the version number of the binary serialization format. All
	// prior versions up to and including this one are deserializable by this
	// package.
	Version = 1
)

var ErrIncompatibleVersion = errors.New("hll: incompatible version")

// New creates a new hyperloglog++ counter with the specified precision. p must be in the range [4,18]
func New(p uint8) *Counter {
	if p < 4 || p > 18 {
		panic("hll: precision p must be in range [4,18]")
	}
	m := int(1 << uint(p))
	c := &Counter{
		p:         p,
		mask:      ((1 << p) - 1) << (64 - p),
		bits:      bitbucket.New(m, 6),
		threshold: thresholds[p],
	}

	if m <= 16 {
		c.alpha = 0.673
	} else if m <= 32 {
		c.alpha = 0.697
	} else if m <= 64 {
		c.alpha = 0.709
	} else if m > 256 {
		c.alpha = 0.7213475 // 1/(2log2)
	} else {
		c.alpha = 0.7213 / (1 + 1.079/float64(m))
	}

	return c
}

type Counter struct {
	p    uint8 // precision
	mask uint64
	bits *bitbucket.BitBucket

	alpha     float64
	threshold float64
}

func (c *Counter) Add(v []byte) {
	h := fnv.Sum64(v)
	idx := int((h & c.mask) >> (64 - c.p))

	// Count leading zeros - there are faster ways to do this
	rho := uint8(1)
	t := uint64(1 << (63 - c.p))
	for i := 0; i < 64-int(c.p); i++ {
		if h&t != 0 {
			break
		}
		rho++
		t >>= 1
	}
	if rho > c.bits.Get(idx) {
		c.bits.Set(idx, rho)
	}
}

// Count returns an estimate of the number of distinct items that have been added to the counter.
func (c *Counter) Count() int64 {
	h := 0.0
	m := int(uint(1) << uint(c.p))

	// See if we can get away with linear counting
	v := c.bits.CountN(0)
	if v > 0 {
		// Count is in the low region so try linear counting
		h = math.Ceil(float64(m) * math.Log(float64(m)/float64(v)))
		if h <= c.threshold {
			return int64(h)
		}
	}

	z := c.harmonic(m)

	// Check if we are in the biased region of the standard hyperloglog algorithm
	if z >= c.alpha*float64(m)/7.0 {
		// Use a more precise, but slower calculation.
		z = c.poisson()
	}

	// Normalize the indicator
	h = c.alpha * float64(m) * float64(m) / z
	return int64(math.Ceil(h))
}

func (c *Counter) LinearCount() int64 {
	v := c.bits.CountN(0)
	m := int(uint(1) << uint(c.p))
	h := float64(m) * math.Log(float64(m)/float64(v))
	return int64(math.Ceil(h))
}

// harmonic returns an indicator of the number of distinct items that have been
// added to the counter using a harmonic mean as per Flajolet, Philippe; Fusy,
// Éric; Gandouet, Olivier; Meunier, Frédéric (2007) "HyperLogLog: the
// analysis of a near-optimal cardinality estimation algorithm", Proceedings
// of the 2007 Conference on Analysis of Algorithms
func (c *Counter) harmonic(m int) float64 {
	z := 0.0
	for i := 0; i < m; i++ {
		reg := c.bits.Get(i)
		if reg == 0 {
			z += 0.5
			continue
		}

		delta := 1.0 / float64(uint64(1)<<reg)
		z += delta
	}

	return z
}

func (c *Counter) HarmonicCount() int64 {
	m := int(uint(1) << uint(c.p))
	z := c.harmonic(m)
	h := c.alpha * float64(m) * float64(m) / z
	return int64(math.Ceil(h))
}

// poisson returns an indicator of the number of distinct items that
// have been added to the counter using a Poisson approximation as per Ertl,
// Otmar (2016) "New cardinality estimation algorithms for HyperLogLog
// sketches". Available from http://oertl.github.io/hyperloglog-sketch-
// estimation-paper
func (c *Counter) poisson() float64 {
	var cn [64]int

	m := int(uint(1) << uint(c.p))
	q := 64 - int(c.p)
	for i := 0; i < m; i++ {
		reg := c.bits.Get(i)
		cn[int(reg)]++
	}

	z := float64(m) * tau(cn[q+1], m)

	for k := q; k >= 1; k-- {
		z = 0.5 * (z + float64(cn[k]))
	}

	z = z + float64(m)*sigma(cn[0], m)
	return z

}

func (c *Counter) PoissonCount() int64 {
	m := int(uint(1) << uint(c.p))
	z := c.poisson()
	h := c.alpha * float64(m) * float64(m) / z
	return int64(math.Ceil(h))
}

// TODO: pre-compute sigma values
func sigma(c int, m int) float64 {
	x := float64(c) / float64(m)
	if x == 1 {
		return math.Inf(1)
	}

	y := 1.0
	z := x
	for {
		x = x * x
		z1 := z
		z = z + x*y
		y = 2 * y
		if z == z1 {
			break
		}
	}

	return z
}

// TODO: pre-compute tau values
func tau(c int, m int) float64 {
	x := 1 - float64(c)/float64(m)
	y := 1.0
	z := 0.0
	for {
		x = math.Sqrt(x)
		z1 := z
		y = 0.5 * y
		z = z + (1-x)*x*y
		if z == z1 {
			break
		}
	}

	return z
}

var (
	// Linear counting thresholds calculated empirically at http://goo.gl/iU8Ig
	thresholds = map[uint8]float64{
		4:  10,
		5:  20,
		6:  40,
		7:  80,
		8:  220,
		9:  400,
		10: 900,
		11: 1800,
		12: 3100,
		13: 6500,
		14: 11500,
		15: 20000,
		16: 50000,
		17: 120000,
		18: 350000,
	}
)
