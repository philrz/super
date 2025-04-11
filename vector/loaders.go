package vector

import (
	"net/netip"
	"sync"

	"github.com/brimdata/super/vector/bitvec"
)

type (
	BytesLoader interface {
		Load() (BytesTable, bitvec.Bits)
	}
	BitsLoader interface {
		Load() (bitvec.Bits, bitvec.Bits)
	}
	DictLoader interface {
		Load() ([]byte, []uint32, bitvec.Bits)
	}
	FloatLoader interface {
		Load() ([]float64, bitvec.Bits)
	}
	IntLoader interface {
		Load() ([]int64, bitvec.Bits)
	}
	IPLoader interface {
		Load() ([]netip.Addr, bitvec.Bits)
	}
	NetLoader interface {
		Load() ([]netip.Prefix, bitvec.Bits)
	}
	NullsLoader interface {
		Load() bitvec.Bits
	}
	PrimitiveLoader interface {
		Load() (any, bitvec.Bits)
	}
	UintLoader interface {
		Load() ([]uint64, bitvec.Bits)
	}
	Uint32Loader interface {
		Load() ([]uint32, bitvec.Bits)
	}
)

type lock struct {
	mu   sync.RWMutex
	done bool
	any  Any
}

func newLock(any Any) *lock {
	return &lock{any: any}
}

func (l *lock) check() {
	if l != nil {
		l.mu.RLock()
		if !l.done {
			l.mu.RUnlock()
			l.mu.Lock()
			if !l.done {
				l.any.load()
				l.done = true
			}
			l.mu.Unlock()
		} else {
			l.mu.RUnlock()
		}
	}
}
