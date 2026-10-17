package loader

import (
	"slices"
	"sync"

	"github.com/superdb/super"
	"github.com/superdb/super/bsup"
	"github.com/superdb/super/pkg/field"
	"github.com/superdb/super/vector"
)

type record struct {
	mu     sync.Mutex
	meta   *bsup.Record
	len    uint32
	fields []shadow
}

func newRecord(bctx *bsup.Context, meta *bsup.Record) *record {
	fields := make([]shadow, len(meta.Fields))
	len := meta.Len(bctx)
	return &record{
		meta:   meta,
		len:    len,
		fields: fields,
	}
}

func (r *record) length() uint32 {
	return r.len
}

func (r *record) unmarshal(bctx *bsup.Context, projection field.Projection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(projection) == 0 {
		// Unmarshal all the fields of this record.  We're either loading all on demand (nil paths)
		// or loading this record because it's referenced at the end of a projected path.
		for k := range r.fields {
			if r.fields[k] == nil {
				r.fields[k] = newShadow(bctx, r.meta.Fields[k].Values)
			}
			r.fields[k].unmarshal(bctx, nil)
		}
		return
	}
	for _, node := range projection {
		if k := indexOfField(node.Name, r.meta); k >= 0 {
			if r.fields[k] == nil {
				r.fields[k] = newShadow(bctx, r.meta.Fields[k].Values)
			}
			r.fields[k].unmarshal(bctx, node.Proj)
		}
	}
}

func (r *record) project(loader *FrameLoader, projection field.Projection) vector.Any {
	valFields := make([]vector.Any, 0, len(r.fields))
	types := make([]super.Field, 0, len(r.fields))
	if len(projection) == 0 {
		// Build the whole record.  We're either loading all on demand (nil paths)
		// or loading this record because it's referenced at the end of a projected path.
		for k := range r.fields {
			val := r.fields[k].project(loader, nil)
			valFields = append(valFields, val)
			types = append(types, super.NewField(r.meta.Fields[k].Name, val.Type()))
		}
		return vector.NewRecord(loader.sctx.MustLookupTypeRecord(types), valFields, r.length())
	}
	fields := make([]super.Field, 0, len(r.fields))
	for k := range r.fields {
		if child, ok := project(r.meta.Fields[k].Name, projection); ok {
			val := r.fields[k].project(loader, child)
			valFields = append(valFields, val)
			types = append(types, super.NewField(r.meta.Fields[k].Name, val.Type()))
		} else {
			// XXX problem is we need the type to build a vector.Loader but currently
			// we have no way to get the type without loading the children.  I have
			// another design in mind for the column metas that will have a better
			// type representation so I will finish that redesign before filling
			// this in here.
			var badType super.Type
			var bctx *bsup.Context
			valFields = append(valFields, vector.NewUnloaded(badType, r.meta.Len(bctx)))
			types = append(types, super.NewField(r.meta.Fields[k].Name, badType))
		}
	}
	return vector.NewRecord(loader.sctx.MustLookupTypeRecord(fields), valFields, r.length())
}

func project(name string, projection field.Projection) (field.Projection, bool) {
	//XXX this is O(n*n), for large, wide records this is problematic; we need to
	// recode this to use a map (maybe put the map inside field.Projection)
	for _, node := range projection {
		if node.Name == name {
			return node.Proj, true
		}
	}
	return nil, false
}

func indexOfField(name string, r *bsup.Record) int {
	return slices.IndexFunc(r.Fields, func(f bsup.Field) bool {
		return f.Name == name
	})
}
