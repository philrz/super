package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Uint struct {
	l      *lock
	loader UintLoader
	Typ    super.Type
	values []uint64
	nulls  bitvec.Bits
	length uint32
}

var _ Any = (*Uint)(nil)
var _ Promotable = (*Uint)(nil)

func NewUint(typ super.Type, values []uint64, nulls bitvec.Bits) *Uint {
	return &Uint{Typ: typ, values: values, nulls: nulls, length: uint32(len(values))}
}

func NewUintEmpty(typ super.Type, length uint32, nulls bitvec.Bits) *Uint {
	return NewUint(typ, make([]uint64, 0, length), nulls)
}

func NewLazyUint(typ super.Type, length uint32, loader UintLoader) *Uint {
	u := &Uint{Typ: typ, length: length, loader: loader}
	u.l = newLock(u)
	return u
}

func (u *Uint) Append(v uint64) {
	u.values = append(u.values, v)
	u.length = uint32(len(u.values))
}

func (u *Uint) Type() super.Type {
	return u.Typ
}

func (u *Uint) Len() uint32 {
	return u.length
}

func (u *Uint) load() {
	u.values, u.nulls = u.loader.Load()
}

func (u *Uint) Value(slot uint32) uint64 {
	return u.Values()[slot]
}

func (u *Uint) Values() []uint64 {
	u.l.check()
	return u.values
}

func (u *Uint) Nulls() bitvec.Bits {
	u.l.check()
	return u.nulls
}

func (u *Uint) SetNulls(nulls bitvec.Bits) {
	u.nulls = nulls
}

func (u *Uint) Serialize(b *zcode.Builder, slot uint32) {
	if u.Nulls().IsSet(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeUint(u.Values()[slot]))
	}
}

func (u *Uint) Promote(typ super.Type) Promotable {
	copy := *u
	copy.Typ = typ
	return &copy
}

func UintValue(vec Any, slot uint32) (uint64, bool) {
	switch vec := Under(vec).(type) {
	case *Uint:
		return vec.Value(slot), vec.Nulls().IsSet(slot)
	case *Const:
		return vec.Value().Ptr().Uint(), vec.Nulls().IsSet(slot)
	case *Dict:
		if vec.Nulls().IsSet(slot) {
			return 0, true
		}
		return UintValue(vec.Any, uint32(vec.Index()[slot]))
	case *Dynamic:
		tag := vec.Tags()[slot]
		return UintValue(vec.Values[tag], vec.TagMap().Forward[slot])
	case *View:
		return UintValue(vec.Any, vec.Index()[slot])
	}
	panic(vec)
}
