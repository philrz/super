package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Set struct {
	l       *lock
	loader  Uint32Loader
	Typ     *super.TypeSet
	Values  Any
	nulls   bitvec.Bits
	offsets []uint32
	length  uint32
}

var _ Any = (*Set)(nil)

func NewSet(typ *super.TypeSet, offsets []uint32, values Any, nulls bitvec.Bits) *Set {
	return &Set{Typ: typ, offsets: offsets, Values: values, nulls: nulls, length: uint32(len(offsets) - 1)}
}

func NewLazySet(typ *super.TypeSet, loader Uint32Loader, values Any, length uint32) *Set {
	s := &Set{Typ: typ, loader: loader, Values: values, length: length}
	s.l = newLock(s)
	return s
}

func (s *Set) Type() super.Type {
	return s.Typ
}

func (s *Set) Len() uint32 {
	return s.length
}

func (s *Set) load() {
	s.offsets, s.nulls = s.loader.Load()
}

func (s *Set) Offsets() []uint32 {
	s.l.check()
	return s.offsets
}

func (s *Set) Nulls() bitvec.Bits {
	s.l.check()
	return s.nulls
}

func (s *Set) SetNulls(nulls bitvec.Bits) {
	s.nulls = nulls
}

func (s *Set) Serialize(b *zcode.Builder, slot uint32) {
	if s.Nulls().IsSet(slot) {
		b.Append(nil)
		return
	}
	offs := s.Offsets()
	off := offs[slot]
	b.BeginContainer()
	for end := offs[slot+1]; off < end; off++ {
		s.Values.Serialize(b, off)
	}
	b.TransformContainer(super.NormalizeSet)
	b.EndContainer()
}
