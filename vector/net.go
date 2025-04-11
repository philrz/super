package vector

import (
	"net/netip"

	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Net struct {
	l      *lock
	loader NetLoader
	values []netip.Prefix
	nulls  bitvec.Bits
	length uint32
}

var _ Any = (*Net)(nil)

func NewNet(values []netip.Prefix, nulls bitvec.Bits) *Net {
	return &Net{values: values, nulls: nulls, length: uint32(len(values))}
}

func NewLazyNet(loader NetLoader, length uint32) *Net {
	n := &Net{loader: loader, length: length}
	n.l = newLock(n)
	return n
}

func (n *Net) Type() super.Type {
	return super.TypeNet
}

func (n *Net) Len() uint32 {
	return n.length
}

func (n *Net) load() {
	n.values, n.nulls = n.loader.Load()
}

func (n *Net) Values() []netip.Prefix {
	n.l.check()
	return n.values
}

func (n *Net) Nulls() bitvec.Bits {
	n.l.check()
	return n.nulls
}

func (n *Net) SetNulls(nulls bitvec.Bits) {
	n.nulls = nulls
}

func (n *Net) Serialize(b *zcode.Builder, slot uint32) {
	if n.Nulls().IsSet(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeNet(n.Values()[slot]))
	}
}

func NetValue(val Any, slot uint32) (netip.Prefix, bool) {
	switch val := val.(type) {
	case *Net:
		return val.Values()[slot], val.Nulls().IsSet(slot)
	case *Const:
		if val.Nulls().IsSet(slot) {
			return netip.Prefix{}, true
		}
		s, _ := val.AsBytes()
		return super.DecodeNet(s), false
	case *Dict:
		if val.Nulls().IsSet(slot) {
			return netip.Prefix{}, true
		}
		slot = uint32(val.Index()[slot])
		return val.Any.(*Net).Values()[slot], false
	case *View:
		slot = val.Index()[slot]
		return NetValue(val.Any, slot)
	}
	panic(val)
}
