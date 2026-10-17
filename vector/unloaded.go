package vector

import (
	"github.com/superdb/super"
	"github.com/superdb/super/scode"
)

type Unloaded struct {
	typ super.Type
	len uint32
}

func NewUnloaded(typ super.Type, len uint32) *Unloaded {
	return &Unloaded{typ, len}
}

func (u *Unloaded) Kind() Kind {
	return typeToKind(u.typ)
}

func (u *Unloaded) Type() super.Type {
	return u.typ
}

func (u *Unloaded) Len() uint32 {
	return u.len
}

func (u *Unloaded) Serialize(*scode.Builder, uint32) {
	panic("unloaded vector")
}
