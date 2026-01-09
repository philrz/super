package expr

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector"
	"github.com/brimdata/super/vector/bitvec"
)

type Literal struct {
	val super.Value
}

var _ Evaluator = (*Literal)(nil)

func NewLiteral(val super.Value) *Literal {
	return &Literal{val: val}
}

func (l Literal) Eval(vec vector.Any) vector.Any {
	return buildLiteral(l.val, vec.Len())
}

func buildLiteral(val super.Value, n uint32) vector.Any {
	typ := val.Type()
	if named, ok := typ.(*super.TypeNamed); ok {
		val := super.NewValue(named.Type, val.Bytes())
		return vector.NewNamed(named, buildLiteral(val, n))
	}
	var nulls bitvec.Bits
	if val.IsNull() {
		nulls = bitvec.NewTrue(n)
	}
	if super.IsPrimitiveType(typ) {
		return vector.NewConst(val, n, nulls)
	}
	b := vector.NewBuilder(typ)
	b.Write(val.Bytes())
	out := b.Build(nulls)
	return vector.Pick(out, make([]uint32, n))
}
