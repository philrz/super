package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/vector/bitvec"
	"github.com/brimdata/super/zcode"
)

type Record struct {
	l      *lock
	loader NullsLoader
	Typ    *super.TypeRecord
	fields []Any
	length uint32
	nulls  bitvec.Bits
}

var _ Any = (*Record)(nil)

func NewRecord(typ *super.TypeRecord, fields []Any, length uint32, nulls bitvec.Bits) *Record {
	return &Record{Typ: typ, fields: fields, length: length, nulls: nulls}
}

func NewLazyRecord(typ *super.TypeRecord, loader NullsLoader, fields []Any, length uint32) *Record {
	r := &Record{Typ: typ, loader: loader, fields: fields, length: length}
	r.l = newLock(r)
	return r
}

func (r *Record) Type() super.Type {
	return r.Typ
}

func (r *Record) Len() uint32 {
	return r.length
}

func (r *Record) load() {
	r.nulls = r.loader.Load()
}

func (r *Record) Fields() []Any {
	r.l.check()
	return r.fields
}

func (r *Record) Nulls() bitvec.Bits {
	r.l.check()
	return r.nulls
}

func (r *Record) SetNulls(nulls bitvec.Bits) {
	r.nulls = nulls
}

func (r *Record) Serialize(b *zcode.Builder, slot uint32) {
	if r.Nulls().IsSet(slot) {
		b.Append(nil)
		return
	}
	b.BeginContainer()
	for _, f := range r.fields {
		f.Serialize(b, slot)
	}
	b.EndContainer()
}
