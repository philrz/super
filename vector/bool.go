package vector

import (
	"math/bits"
	"slices"
	"strings"

	"github.com/brimdata/super"
	"github.com/brimdata/super/zcode"
)

type Bool struct {
	loader Loader
	bits   []uint64
	length uint32
	Nulls  *Bool
}

var _ Any = (*Bool)(nil)

func NewBool(bits []uint64, length uint32, nulls *Bool) *Bool {
	return &Bool{length: length, bits: bits, Nulls: nulls}
}

func NewBoolEmpty(length uint32, nulls *Bool) *Bool {
	b := NewFalse2(length)
	b.Nulls = nulls
	return b
}

func NewFalse2(length uint32) *Bool {
	return &Bool{length: length, bits: make([]uint64, (length+63)/64)}
}

func NewTrue(n uint32) *Bool {
	b := NewFalse2(n)
	for i := range b.bits {
		b.bits[i] = ^uint64(0)
	}
	return b
}

func (b *Bool) Type() super.Type {
	return super.TypeBool
}

func (b *Bool) GetBits() []uint64 {
	if b.bits == nil {
		b.bits = b.loader.Load().([]uint64)
	}
	return b.bits
}

func (b *Bool) Value(slot uint32) bool {
	// Because Bool is used to store nulls for many vectors and it is often
	// nil check to see if receiver is nil and return false.
	if b == nil {
		return false
	}
	bits := b.GetBits()
	return (bits[slot>>6] & (1 << (slot & 0x3f))) != 0
}

func (b *Bool) Set(slot uint32) {
	b.bits[slot>>6] |= (1 << (slot & 0x3f))
}

func (b *Bool) SetLen(length uint32) {
	if b != nil {
		b.length = length
	}
}

func (b *Bool) Len() uint32 {
	if b == nil {
		return 0
	}
	return b.length
}

func (b *Bool) CopyWithBits(bits []uint64) *Bool {
	out := *b
	out.bits = bits
	return &out
}

func (b *Bool) Serialize(builder *zcode.Builder, slot uint32) {
	if b != nil && b.Nulls.Value(slot) {
		builder.Append(nil)
	} else {
		builder.Append(super.EncodeBool(b.Value(slot)))
	}
}

func (b *Bool) TrueCount() uint32 {
	if b == nil {
		return 0
	}
	var n uint32
	for _, bs := range b.bits {
		n += uint32(bits.OnesCount64(bs))
	}
	if numTailBits := b.Len() % 64; numTailBits > 0 {
		mask := ^uint64(0) << numTailBits
		unusedBits := b.bits[len(b.bits)-1] & mask
		n -= uint32(bits.OnesCount64(unusedBits))
	}
	return n
}

// helpful to have around for debugging
func (b *Bool) String() string {
	var s strings.Builder
	if b == nil || b.Len() == 0 {
		return "empty"
	}
	for k := uint32(0); k < b.Len(); k++ {
		if b.Value(k) {
			s.WriteByte('1')
		} else {
			s.WriteByte('0')
		}
	}
	return s.String()
}

func Not(a *Bool) *Bool {
	if a == nil {
		panic("not: nil bool")
	}
	bits := slices.Clone(a.bits) //XXX why clone then clobber the copy?
	for i := range bits {
		bits[i] = ^a.bits[i]
	}
	return a.CopyWithBits(bits)
}

func Or(a, b *Bool) *Bool {
	if b == nil {
		return a
	}
	if a == nil {
		return b
	}
	if a.Len() != b.Len() {
		panic("or'ing two different length bool vectors")
	}
	out := NewFalse2(a.Len())
	for i := range len(a.bits) {
		out.bits[i] = a.bits[i] | b.bits[i]
	}
	return out
}

func And(a, b *Bool) *Bool {
	if b == nil {
		return nil
	}
	if a == nil {
		return nil
	}
	if a.Len() != b.Len() {
		panic("and'ing two different length bool vectors")
	}
	out := NewFalse2(a.Len())
	for i := range len(a.bits) {
		out.bits[i] = a.bits[i] & b.bits[i]
	}
	return out
}

// BoolValue returns the value of slot in vec if the value is a Boolean.  It
// returns false otherwise.
func BoolValue(vec Any, slot uint32) (bool, bool) {
	switch vec := Under(vec).(type) {
	case *Bool:
		return vec.Value(slot), vec.Nulls.Value(slot)
	case *Const:
		return vec.Value().Ptr().AsBool(), vec.Nulls.Value(slot)
	case *Dict:
		if vec.Nulls.Value(slot) {
			return false, true
		}
		return BoolValue(vec.Any, uint32(vec.Index[slot]))
	case *Dynamic:
		tag := vec.Tags[slot]
		return BoolValue(vec.Values[tag], vec.TagMap.Forward[slot])
	case *View:
		return BoolValue(vec.Any, vec.Index[slot])
	}
	panic(vec)
}

func NullsOf(v Any) *Bool {
	switch v := v.(type) {
	case *Array:
		return v.Nulls
	case *Bytes:
		return v.Nulls
	case *Bool:
		if v != nil {
			return v.Nulls
		}
		return nil
	case *Const:
		if v.Value().IsNull() {
			return NewTrue(v.Len())
		}
		return v.Nulls
	case *Dict:
		return v.Nulls
	case *Enum:
		return v.Nulls
	case *Error:
		return Or(v.Nulls, NullsOf(v.Vals))
	case *Float:
		return v.Nulls
	case *Int:
		return v.Nulls
	case *IP:
		return v.Nulls
	case *Map:
		return v.Nulls
	case *Named:
		return NullsOf(v.Any)
	case *Net:
		return v.Nulls
	case *Record:
		return v.Nulls
	case *Set:
		return v.Nulls
	case *String:
		return v.Nulls
	case *TypeValue:
		return v.Nulls
	case *Uint:
		return v.Nulls
	case *Union:
		return v.Nulls
	case *View:
		return NewBoolView(NullsOf(v.Any), v.Index)
	}
	panic(v)
}

func CopyAndSetNulls(v Any, nulls *Bool) Any {
	switch v := v.(type) {
	case *Array:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Bytes:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Bool:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Const:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Dict:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Enum:
		return &Enum{
			Typ:  v.Typ,
			Uint: CopyAndSetNulls(v.Uint, nulls).(*Uint),
		}
	case *Error:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Float:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Int:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *IP:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Map:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Named:
		return &Named{
			Typ: v.Typ,
			Any: CopyAndSetNulls(v.Any, nulls),
		}
	case *Net:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Record:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Set:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *String:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *TypeValue:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Uint:
		copy := *v
		copy.Nulls = nulls
		return &copy
	case *Union:
		return NewUnion(v.Typ, v.Tags, v.Values, nulls)
	default:
		panic(v)
	}
}
