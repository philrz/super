package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/zcode"
)

type String struct {
	loader Loader
	table  BytesTable
	length uint32
	Nulls  *Bool
}

var _ Any = (*String)(nil)

func NewString(table BytesTable, nulls *Bool) *String {
	return &String{table: table, length: table.Len(), Nulls: nulls}
}

func NewStringEmpty(cap uint32, nulls *Bool) *String {
	return NewString(NewBytesTableEmpty(cap), nulls)
}

func (s *String) Append(v string) {
	s.table.bytes = append(s.table.bytes, v...)
	s.table.offsets = append(s.table.offsets, uint32(len(s.table.bytes)))
	s.length++
}

func (s *String) Type() super.Type {
	return super.TypeString
}

func (s *String) Len() uint32 {
	return s.length
}

func (s *String) Table() BytesTable {
	if s.table.offsets == nil {
		s.table = s.loader.Load().(BytesTable)
	}
	return s.table
}

func (s *String) Value(slot uint32) string {
	return s.Table().String(slot)
}

func (s *String) Serialize(b *zcode.Builder, slot uint32) {
	if s.Nulls.Value(slot) {
		b.Append(nil)
	} else {
		b.Append(super.EncodeString(s.Value(slot)))
	}
}

func StringValue(val Any, slot uint32) (string, bool) {
	switch val := val.(type) {
	case *String:
		if val.Nulls.Value(slot) {
			return "", true
		}
		return val.Value(slot), false
	case *Const:
		if val.Nulls.Value(slot) {
			return "", true
		}
		s, _ := val.AsString()
		return s, false
	case *Dict:
		if val.Nulls.Value(slot) {
			return "", true
		}
		return StringValue(val.Any, uint32(val.Index[slot]))
	case *View:
		return StringValue(val.Any, val.Index[slot])
	}
	panic(val)
}
