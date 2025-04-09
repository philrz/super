package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/zcode"
)

type Int struct {
	Typ    super.Type
	Nulls  *Bool
	loader Loader
	values []int64
	length uint32
}

var _ Any = (*Int)(nil)
var _ Promotable = (*Int)(nil)

func NewInt(typ super.Type, values []int64, nulls *Bool) *Int {
	length := uint32(len(values))
	return &Int{Typ: typ, values: values, length: length, Nulls: nulls}
}

func NewIntLoader(typ super.Type, loader Loader, length uint32, nulls *Bool) *Int {
	return &Int{Typ: typ, loader: loader, length: length, Nulls: nulls}
}

// XXX
func NewIntEmpty(typ super.Type, length uint32, nulls *Bool) *Int {
	return NewInt(typ, make([]int64, 0, length), nulls)
}

func (i *Int) Append(v int64) {
	i.values = append(i.values, v)
}

func (i *Int) Type() super.Type {
	return i.Typ
}

func (i *Int) Len() uint32 {
	return i.length
}

func (i *Int) Values() []int64 {
	if i.values == nil {
		i.values = i.loader.Load().([]int64)
	}
	return i.values
}

func (i *Int) Value(slot uint32) int64 {
	return i.Values()[slot]
}

func (i *Int) Serialize(b *zcode.Builder, slot uint32) {
	if i.Nulls.Value(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeInt(i.Value(slot)))
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
		return vec.Value(slot), vec.Nulls.Value(slot)
	case *Const:
		return vec.val.Int(), vec.Nulls.Value(slot)
	case *Dict:
		if vec.Nulls.Value(slot) {
			return 0, true
		}
		return IntValue(vec.Any, uint32(vec.Index[slot]))
	case *Dynamic:
		tag := vec.Tags[slot]
		return IntValue(vec.Values[tag], vec.TagMap.Forward[slot])
	case *View:
		return IntValue(vec.Any, vec.Index[slot])
	}
	panic(vec)
}
