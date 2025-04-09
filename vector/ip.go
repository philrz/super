package vector

import (
	"net/netip"

	"github.com/brimdata/super"
	"github.com/brimdata/super/zcode"
)

type IP struct {
	loader Loader
	values []netip.Addr
	length uint32
	Nulls  *Bool
}

var _ Any = (*IP)(nil)

func NewIP(values []netip.Addr, nulls *Bool) *IP {
	return &IP{values: values, length: uint32(len(values)), Nulls: nulls}
}

func NewIPLoader(loader Loader, length uint32, nulls *Bool) *IP {
	return &IP{loader: loader, length: length, Nulls: nulls}
}

func (i *IP) Type() super.Type {
	return super.TypeIP
}

func (i *IP) Len() uint32 {
	return i.length
}

func (i *IP) Values() []netip.Addr {
	if i.values == nil {
		i.values = i.loader.Load().([]netip.Addr)
		if uint32(len(i.values)) != i.length {
			panic("vector.IP bad length")
		}
	}
	return i.values
}

func (i *IP) Serialize(b *zcode.Builder, slot uint32) {
	if i.Nulls.Value(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeIP(i.Values()[slot]))
	}
}

func IPValue(val Any, slot uint32) (netip.Addr, bool) {
	switch val := val.(type) {
	case *IP:
		return val.Values()[slot], val.Nulls.Value(slot)
	case *Const:
		if val.Nulls.Value(slot) {
			return netip.Addr{}, true
		}
		b, _ := val.AsBytes()
		return super.DecodeIP(b), false
	case *Dict:
		if val.Nulls.Value(slot) {
			return netip.Addr{}, true
		}
		slot = uint32(val.Index[slot])
		return val.Any.(*IP).Values()[slot], false
	case *View:
		slot = val.Index[slot]
		return IPValue(val.Any, slot)
	}
	panic(val)
}
