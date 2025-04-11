package vector

import (
	"slices"

	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Union struct {
	l       *lock
	loader  Uint32Loader
	dynamic *Dynamic
	Typ     *super.TypeUnion
	nulls   bitvec.Bits
	length  uint32
	vals    []Any // a place to hold values during lazy construction
}

var _ Any = (*Union)(nil)

func NewUnion(typ *super.TypeUnion, tags []uint32, vals []Any, nulls bitvec.Bits) *Union {
	return &Union{dynamic: NewDynamic(tags, vals), Typ: typ, nulls: nulls, length: uint32(len(tags))}
}

func NewLazyUnion(typ *super.TypeUnion, loader Uint32Loader, vals []Any, length uint32) *Union {
	u := &Union{Typ: typ, loader: loader, vals: vals, length: length}
	u.l = newLock(u)
	return u
}

func (u *Union) Type() super.Type {
	return u.Typ
}

func (u *Union) Len() uint32 {
	return u.length
}

func (u *Union) Values() []Any {
	u.l.check()
	return u.dynamic.Values
}

func (u *Union) Deunion() *Dynamic {
	u.l.check()
	return u.dynamic
}

func (u *Union) Tags() []uint32 {
	u.l.check()
	return u.dynamic.Tags()
}

func (u *Union) TagMap() *TagMap {
	u.l.check()
	return u.dynamic.TagMap()
}

func (u *Union) Nulls() bitvec.Bits {
	u.l.check()
	return u.nulls
}

func (u *Union) load() {
	tags, nulls := u.loader.Load()
	u.nulls = nulls
	tags, vals := FlattenUnionNulls(u.Typ, tags, u.vals, nulls)
	u.vals = nil
	u.dynamic = NewDynamic(tags, vals)
}

func (u *Union) Serialize(b *zcode.Builder, slot uint32) {
	u.l.check()
	if u.nulls.IsSet(slot) {
		b.Append(nil)
		return
	}
	b.BeginContainer()
	tag := u.Typ.TagOf(u.dynamic.TypeOf(slot))
	b.Append(super.EncodeInt(int64(tag)))
	u.dynamic.Serialize(b, slot)
	b.EndContainer()
}

func Deunion(vec Any) Any {
	if u, ok := vec.(*Union); ok {
		return addUnionNullsToDynamic(u.Typ, NewDynamic(u.Tags(), u.dynamic.Values), u.Nulls())
	}
	return vec
}

func FlattenUnionNulls(typ *super.TypeUnion, tags []uint32, vecs []Any, nulls bitvec.Bits) ([]uint32, []Any) {
	if nulls.IsZero() {
		return tags, vecs
	}
	var newtags []uint32
	n := uint32(len(vecs))
	var nullcount uint32
	for i := range nulls.Len() {
		if nulls.IsSet(i) {
			newtags = append(newtags, n)
			nullcount++
		} else {
			newtags = append(newtags, tags[0])
			tags = tags[1:]
		}
	}
	return newtags, append(vecs, NewConst(super.NewValue(typ, nil), nullcount, bitvec.Zero))
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
	tags := slices.Clone(d.Tags())
	for i := range nulls.Len() {
		if nulls.IsSetDirect(i) {
			if tags[i] != uint32(nullTag) {
				rebuild = true
				// If value was not previously null delete value from vector.
				delIndexes[tags[i]] = append(delIndexes[tags[i]], d.TagMap().Forward[i])
			}
			tags[i] = uint32(nullTag)
			count++
		}
	}
	vals[nullTag].(*Const).length = count
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
