package vector

import (
	"net/netip"

	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type IP struct {
	l      *lock
	loader IPLoader
	values []netip.Addr
	nulls  bitvec.Bits
	length uint32
}

var _ Any = (*IP)(nil)

func NewIP(values []netip.Addr, nulls bitvec.Bits) *IP {
	return &IP{values: values, nulls: nulls, length: uint32(len(values))}
}

func NewLazyIP(loader IPLoader, length uint32) *IP {
	i := &IP{loader: loader, length: length}
	i.l = newLock(i)
	return i
}

func (i *IP) Type() super.Type {
	return super.TypeIP
}

func (i *IP) Len() uint32 {
	return i.length
}

func (i *IP) load() {
	i.values, i.nulls = i.loader.Load()
}

func (i *IP) Values() []netip.Addr {
	i.l.check()
	return i.values
}

func (i *IP) Nulls() bitvec.Bits {
	i.l.check()
	return i.nulls
}

func (i *IP) SetNulls(nulls bitvec.Bits) {
	i.nulls = nulls
}

func (i *IP) Serialize(b *zcode.Builder, slot uint32) {
	if i.Nulls().IsSet(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeIP(i.Values()[slot]))
	}
}

func IPValue(val Any, slot uint32) (netip.Addr, bool) {
	switch val := val.(type) {
	case *IP:
		return val.Values()[slot], val.Nulls().IsSet(slot)
	case *Const:
		if val.Nulls().IsSet(slot) {
			return netip.Addr{}, true
		}
		b, _ := val.AsBytes()
		return super.DecodeIP(b), false
	case *Dict:
		if val.Nulls().IsSet(slot) {
			return netip.Addr{}, true
		}
		slot = uint32(val.Index()[slot])
		return val.Any.(*IP).Values()[slot], false
	case *View:
		slot = val.Index()[slot]
		return IPValue(val.Any, slot)
	}
	panic(val)
}
