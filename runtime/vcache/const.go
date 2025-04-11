package vcache

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/csup"
	"github.com/brimdata/super/pkg/field"
	"github.com/brimdata/super/vector"
	"github.com/brimdata/super/vector/bitvec"
)

type const_ struct {
	meta *csup.Const
	count
	nulls *nulls
}

var _ shadow = (*const_)(nil)

func newConst(cctx *csup.Context, meta *csup.Const, nulls *nulls) *const_ {
	return &const_{
		meta:  meta,
		nulls: nulls,
		count: count{meta.Len(cctx), nulls.count()},
	}
}

func (*const_) unmarshal(*csup.Context, field.Projection) {}

func (c *const_) project(loader *loader, projection field.Projection) vector.Any {
	if len(projection) > 0 {
		return vector.NewMissing(loader.sctx, c.length())
	}
	return vector.NewConst(c.loadVal(loader), c.length(), c.load(loader))
}

func (c *const_) lazy(loader *loader, projection field.Projection) vector.Any {
	if len(projection) > 0 {
		return vector.NewMissing(loader.sctx, c.length())
	}
	val := c.loadVal(loader)
	return vector.NewLazyConst(val, &constLoader{loader, c}, c.length())
}

func (c *const_) loadVal(loader *loader) super.Value {
	// Map the const super.Value in the csup's type context to
	// a new one in the query type context.
	val := c.meta.Value
	typ, err := loader.sctx.TranslateType(val.Type())
	if err != nil {
		panic(err)
	}
	return super.NewValue(typ, val.Bytes())
}

func (c *const_) load(loader *loader) bitvec.Bits {
	return c.nulls.get(loader)
}

type constLoader struct {
	loader *loader
	shadow *const_
}

var _ vector.NullsLoader = (*constLoader)(nil)

func (c *constLoader) Load() bitvec.Bits {
	return c.shadow.load(c.loader)
}
