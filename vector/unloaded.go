package vector

import "github.com/superdb/super"

type Unloaded struct {
	typ super.Type
	len uint32
}

func NewUnloaded(typ super.Type, len uint32) *Unloaded {
	return &Unloaded{typ, len}
}

func (u *Unloaded) Type() super.Type {
	return u.typ
}

func (u *Unloaded) Len() uint32 {
	return u.len
}
