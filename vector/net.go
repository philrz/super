package vector

import (
	"net/netip"

	"github.com/brimdata/super"
	"github.com/brimdata/super/zcode"
)

type Net struct {
	loader Loader
	values []netip.Prefix
	length uint32
	Nulls  *Bool
}

var _ Any = (*Net)(nil)

func NewNet(values []netip.Prefix, nulls *Bool) *Net {
	return &Net{values: values, length: uint32(len(values)), Nulls: nulls}
}

func NewNetLoad(loader Loader, length uint32, nulls *Bool) *Net {
	return &Net{loader: loader, length: length, Nulls: nulls}
}

func (n *Net) Type() super.Type {
	return super.TypeNet
}

func (n *Net) Len() uint32 {
	return n.length
}

func (n *Net) Values() []netip.Prefix {
	if n.values == nil {
		n.values = n.loader.Load().([]netip.Prefix)
		if uint32(len(n.values)) != n.length {
			panic("vector.Net bad length")
		}
	}
	return n.values
}

func (n *Net) Serialize(b *zcode.Builder, slot uint32) {
	if n.Nulls.Value(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeNet(n.Values()[slot]))
	}
}

func NetValue(val Any, slot uint32) (netip.Prefix, bool) {
	switch val := val.(type) {
	case *Net:
		return val.Values()[slot], val.Nulls.Value(slot)
	case *Const:
		if val.Nulls.Value(slot) {
			return netip.Prefix{}, true
		}
		s, _ := val.AsBytes()
		return super.DecodeNet(s), false
	case *Dict:
		if val.Nulls.Value(slot) {
			return netip.Prefix{}, true
		}
		slot = uint32(val.Index[slot])
		return val.Any.(*Net).Values()[slot], false
	case *View:
		slot = val.Index[slot]
		return NetValue(val.Any, slot)
	}
	panic(val)
}
