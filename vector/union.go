package vector

import (
	"fmt"
	"slices"

	"github.com/brimdata/super"
	"github.com/brimdata/super/sup"
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
	u := &Union{NewDynamic(tags, vals), typ, nulls}
	u.checkInvariant()
	return u
}

func (u *Union) checkInvariant() {
	vals := u.Values
	if len(u.Typ.Types) == len(vals) {
		// Check this type isn't in Values and that every tag is in range.
		for _, v := range u.Values {
			if v.Type() == u.Typ {
				panic(fmt.Sprintf("union invariant violated: type can't be in itself: %s", sup.String(u.Typ)))
			}
		}
		for _, tag := range u.Tags {
			if tag >= uint32(len(vals)) {
				panic("union invariant violated: bad tag")
			}
		}
		return
	}
	if len(u.Typ.Types) != len(vals)-1 {
		panic(fmt.Sprintf("union invariant violated: bad sizes (union types %d, number vals %d)", len(u.Typ.Types), len(vals)))
	}
	if u.Typ != vals[len(vals)-1].Type() {
		s := fmt.Sprintf("union invariant violated: union type not last %s (vs last type %s)\n", sup.String(u.Typ), sup.String(vals[len(vals)-1].Type()))
		for _, v := range vals {
			s += fmt.Sprintf("\t%s\n", sup.String(v.Type()))
		}
		panic(s)
	}
	nullTag := uint32(len(vals) - 1)
	var nullcnt int
	for _, tag := range u.Tags {
		if tag == nullTag {
			nullcnt++
		}
	}
	if nullcnt == 0 {
		s := "union invariant violated: no nulltags when nulls present\n"
		for _, v := range vals {
			s += fmt.Sprintf("\t%s\n", sup.String(v.Type()))
		}
		panic(s)
	}
	if u.Nulls.TrueCount() != uint32(nullcnt) {
		panic(fmt.Sprintf("union invariant violated: %d nulls mask vs %d null tags", u.Nulls.TrueCount(), nullcnt))
	}
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
		return addUnionNullsToDynamic(u.Typ, NewDynamic(u.Tags, u.Values), u.Nulls)
		//return u.Dynamic
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
