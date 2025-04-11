package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
)

type Enum struct {
	*Uint
	Typ *super.TypeEnum
}

func NewEnum(typ *super.TypeEnum, vals []uint64, nulls bitvec.Bits) *Enum {
	return &Enum{
		Typ:  typ,
		Uint: NewUint(super.TypeUint64, vals, nulls),
	}
}

func NewLazyEnum(typ *super.TypeEnum, length uint32, loader UintLoader) *Enum {
	return &Enum{
		Typ:  typ,
		Uint: NewLazyUint(super.TypeUint64, length, loader),
	}
}

func (e *Enum) Type() super.Type { return e.Typ }
