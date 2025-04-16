package vector

import (
	"slices"

	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Union struct {
	*Dynamic
	Typ   *super.TypeUnion
	Nulls bitvec.Bits
}

var _ Any = (*Union)(nil)

// NewUnion creates a new Union vector that preserves the invariant that all null
// values are stored in the underlying Dynamic as a const null of this union's type.
// This allows to reference the union values sparsely with the intervening appearing
// explicitly in the slot of the Dynamic.  It also means we can trivially deunion
// any union by returning its Dynamic.  The other invariant is that if there are nulls
// in the Union then constant vector is stored as the last element of the values array
// and the corresponding union type consists of the remaining types in the values array
// (in natural type order) without this last type  (not in natural type order).
func NewUnion(typ *super.TypeUnion, tags []uint32, vals []Any, nulls bitvec.Bits) *Union {
	return &Union{NewDynamic(tags, vals), typ, nulls}
}

func (u *Union) Type() super.Type {
	return u.Typ
}

func (u *Union) Serialize(b *zcode.Builder, slot uint32) {
	if u.Nulls.IsSet(slot) {
		b.Append(nil)
		return
	}
	b.BeginContainer()
	tag := u.Typ.TagOf(u.TypeOf(slot))
	b.Append(super.EncodeInt(int64(tag)))
	u.Dynamic.Serialize(b, slot)
	b.EndContainer()
}

func Deunion(vec Any) Any {
	if u, ok := vec.(*Union); ok {
		//return addUnionNullsToDynamic(u.Typ, NewDynamic(u.Tags, u.Values), u.Nulls)
		return u.Dynamic
	}
	return vec
}

func isUnionNullsVec(typ *super.TypeUnion, vec Any) bool {
	c, ok := vec.(*Const)
	return ok && c.val.IsNull() && c.val.Type() == typ
}

func addUnionNullsToDynamic(typ *super.TypeUnion, d *Dynamic, nulls bitvec.Bits) *Dynamic {
	if nulls.IsZero() {
		return d
	}
	nullTag := slices.IndexFunc(d.Values, func(vec Any) bool {
		return isUnionNullsVec(typ, vec)
	})
	vals := slices.Clone(d.Values)
	if nullTag == -1 {
		nullTag = len(vals)
		vals = append(vals, NewConst(super.NewValue(typ, nil), 0, bitvec.Zero))
	}
	var rebuild bool
	var count uint32
	delIndexes := make([][]uint32, len(vals))
	tags := slices.Clone(d.Tags)
	for i := range nulls.Len() {
		if nulls.IsSetDirect(i) {
			if tags[i] != uint32(nullTag) {
				rebuild = true
				// If value was not previously null delete value from vector.
				delIndexes[tags[i]] = append(delIndexes[tags[i]], d.TagMap.Forward[i])
			}
			tags[i] = uint32(nullTag)
			count++
		}
	}
	vals[nullTag].(*Const).len = count
	if rebuild {
		for i, delIndex := range delIndexes {
			if len(delIndex) > 0 {
				vals[i] = ReversePick(vals[i], delIndex)
			}
		}
		return NewDynamic(tags, vals)
	}
	return d
}
