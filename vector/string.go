package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/pkg/byteconv"
	"github.com/brimdata/super/zcode"
)

type String struct {
	loader Loader
	table  StringTable
	length uint32
	Nulls  *Bool
}

var _ Any = (*String)(nil)

func NewString(offsets []uint32, bytes []byte, nulls *Bool) *String {
	return &String{table: StringTable{offsets, bytes}, length: uint32(len(offsets) - 1), Nulls: nulls}
}

func NewStringEmpty(length uint32, nulls *Bool) *String {
	return NewString(make([]uint32, 1, length+1), nil, nulls)
}

func (s *String) Append(v string) {
	s.table.Bytes = append(s.table.Bytes, v...)
	s.table.Offsets = append(s.table.Offsets, uint32(len(s.table.Bytes)))
	s.length++
}

func (s *String) Type() super.Type {
	return super.TypeString
}

func (s *String) Len() uint32 {
	return s.length
}

func (s *String) StringTable() StringTable {
	if s.table.Offsets == nil {
		s.table = s.loader.Load().(StringTable)
	}
	return s.table
}

func (s *String) Value(slot uint32) string {
	return s.StringTable().Value(slot)
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

type StringTable struct {
	Offsets []uint32
	Bytes   []byte
}

func (s StringTable) Value(slot uint32) string {
	return string(s.Bytes[s.Offsets[slot]:s.Offsets[slot+1]])
}

func (s StringTable) UnsafeString(slot uint32) string {
	return byteconv.UnsafeString(s.Bytes[s.Offsets[slot]:s.Offsets[slot+1]])
}

func (s StringTable) GetBytes(slot uint32) []byte {
	return s.Bytes[s.Offsets[slot]:s.Offsets[slot+1]]
}
