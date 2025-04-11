package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Map struct {
	l       *lock
	loader  Uint32Loader
	Typ     *super.TypeMap
	Keys    Any
	Values  Any
	nulls   bitvec.Bits
	offsets []uint32
	length  uint32
}

var _ Any = (*Map)(nil)

func NewMap(typ *super.TypeMap, offsets []uint32, keys Any, values Any, nulls bitvec.Bits) *Map {
	return &Map{Typ: typ, offsets: offsets, Keys: keys, Values: values, nulls: nulls, length: uint32(len(offsets) - 1)}
}

func NewLazyMap(typ *super.TypeMap, loader Uint32Loader, keys Any, values Any, length uint32) *Map {
	m := &Map{Typ: typ, loader: loader, Keys: keys, Values: values, length: length}
	m.l = newLock(m)
	return m
}

func (m *Map) Type() super.Type {
	return m.Typ
}

func (m *Map) Len() uint32 {
	return m.length
}

func (m *Map) load() {
	m.offsets, m.nulls = m.loader.Load()
}

func (m *Map) Offsets() []uint32 {
	m.l.check()
	return m.offsets
}

func (m *Map) Nulls() bitvec.Bits {
	m.l.check()
	return m.nulls
}

func (m *Map) SetNulls(nulls bitvec.Bits) {
	m.nulls = nulls
}

func (m *Map) Serialize(b *zcode.Builder, slot uint32) {
	if m.Nulls().IsSet(slot) {
		b.Append(nil)
		return
	}
	offs := m.Offsets()
	off := offs[slot]
	b.BeginContainer()
	for end := offs[slot+1]; off < end; off++ {
		m.Keys.Serialize(b, off)
		m.Values.Serialize(b, off)
	}
	b.TransformContainer(super.NormalizeMap)
	b.EndContainer()
}
