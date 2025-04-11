package vector

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/zcode"
)

// Dynamic is an ordered sequence of values taken from one or more
// hetereogenously-typed vectors.
type Dynamic struct {
	l      *lock
	loader Uint32Loader
	tags   []uint32
	Values []Any
	tagmap *TagMap
	length uint32
}

var _ Any = (*Dynamic)(nil)

func NewDynamic(tags []uint32, values []Any) *Dynamic {
	return &Dynamic{tags: tags, Values: values, tagmap: NewTagMap(tags, values), length: uint32(len(tags))}
}

func NewLazyDynamic(loader Uint32Loader, values []Any, length uint32) *Dynamic {
	d := &Dynamic{loader: loader, Values: values, length: length}
	d.l = newLock(d)
	return d
}

func (*Dynamic) Type() super.Type {
	panic("can't call Type() on a vector.Dynamic")
}

func (d *Dynamic) load() {
	d.tags, _ = d.loader.Load()
	d.tagmap = NewTagMap(d.tags, d.Values)
}

func (d *Dynamic) Tags() []uint32 {
	d.l.check()
	return d.tags
}

func (d *Dynamic) TagMap() *TagMap {
	d.l.check()
	return d.tagmap
}

func (d *Dynamic) TypeOf(slot uint32) super.Type {
	vals := d.Values[d.Tags()[slot]]
	if v2, ok := vals.(*Dynamic); ok {
		return v2.TypeOf(d.TagMap().Forward[slot])
	}
	return vals.Type()
}

func (d *Dynamic) Len() uint32 {
	return d.length
}

func (d *Dynamic) Serialize(b *zcode.Builder, slot uint32) {
	//d.l.check()
	d.Values[d.Tags()[slot]].Serialize(b, d.TagMap().Forward[slot])
}
