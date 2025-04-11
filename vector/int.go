package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Int struct {
	l      *lock
	loader IntLoader
	Typ    super.Type
	nulls  bitvec.Bits
	length uint32
	values []int64
}

var _ Any = (*Int)(nil)
var _ Promotable = (*Int)(nil)

func NewInt(typ super.Type, values []int64, nulls bitvec.Bits) *Int {
	return &Int{Typ: typ, values: values, nulls: nulls, length: uint32(len(values))}
}

func NewLazyInt(typ super.Type, length uint32, loader IntLoader) *Int {
	i := &Int{Typ: typ, length: length, loader: loader}
	i.l = newLock(i)
	return i
}

func NewIntEmpty(typ super.Type, length uint32, nulls bitvec.Bits) *Int {
	return NewInt(typ, make([]int64, 0, length), nulls)
}

func (i *Int) Append(v int64) {
	i.values = append(i.values, v)
	i.length = uint32(len(i.values))
}

func (i *Int) Type() super.Type {
	return i.Typ
}

func (i *Int) Len() uint32 {
	return i.length
}

func (i *Int) Value(slot uint32) int64 {
	return i.Values()[slot]
}

func (i *Int) load() {
	i.values, i.nulls = i.loader.Load()
}

func (i *Int) Values() []int64 {
	i.l.check()
	return i.values
}

func (i *Int) Nulls() bitvec.Bits {
	i.l.check()
	return i.nulls
}

func (i *Int) SetNulls(nulls bitvec.Bits) {
	i.nulls = nulls
}

func (i *Int) Serialize(b *zcode.Builder, slot uint32) {
	if i.Nulls().IsSet(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeInt(i.Values()[slot]))
	}
}

func (i *Int) Promote(typ super.Type) Promotable {
	copy := *i
	copy.Typ = typ
	return &copy
}

func IntValue(vec Any, slot uint32) (int64, bool) {
	switch vec := Under(vec).(type) {
	case *Int:
		return vec.Value(slot), vec.Nulls().IsSet(slot)
	case *Const:
		return vec.val.Int(), vec.Nulls().IsSet(slot)
	case *Dict:
		if vec.Nulls().IsSet(slot) {
			return 0, true
		}
		return IntValue(vec.Any, uint32(vec.Index()[slot]))
	case *Dynamic:
		tag := vec.Tags()[slot]
		return IntValue(vec.Values[tag], vec.TagMap().Forward[slot])
	case *View:
		return IntValue(vec.Any, vec.Index()[slot])
	}
	panic(vec)
}
