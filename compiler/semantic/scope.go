package semantic

import (
	"errors"
	"fmt"
	"strings"

	"github.com/brimdata/super"
	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/compiler/dag"
	"github.com/brimdata/super/compiler/kernel"
	"github.com/brimdata/super/pkg/field"
	"github.com/brimdata/super/zson"
)

type Scope struct {
	parent  *Scope
	nvar    int
	symbols map[string]*entry
	schema  schema
}

func NewScope(parent *Scope) *Scope {
	return &Scope{parent: parent, symbols: make(map[string]*entry)}
}

type entry struct {
	ref   any
	order int
}

func (s *Scope) DefineVar(name *ast.ID) error {
	ref := &dag.Var{
		Kind: "Var",
		Name: name.Name,
		Slot: s.nvars(),
	}
	if err := s.DefineAs(name, ref); err != nil {
		return err
	}
	s.nvar++
	return nil
}

func (s *Scope) DefineAs(name *ast.ID, e any) error {
	if _, ok := s.symbols[name.Name]; ok {
		return fmt.Errorf("symbol %q redefined", name.Name)
	}
	s.symbols[name.Name] = &entry{ref: e, order: len(s.symbols)}
	return nil
}

func (s *Scope) DefineConst(zctx *super.Context, name *ast.ID, def dag.Expr) error {
	val, err := kernel.EvalAtCompileTime(zctx, def)
	if err != nil {
		return err
	}
	if val.IsError() {
		if val.IsMissing() {
			return fmt.Errorf("const %q: cannot have variable dependency", name.Name)
		} else {
			return fmt.Errorf("const %q: %q", name, string(val.Bytes()))
		}
	}
	literal := &dag.Literal{
		Kind:  "Literal",
		Value: zson.FormatValue(val),
	}
	return s.DefineAs(name, literal)
}

func (s *Scope) LookupExpr(name string) (dag.Expr, error) {
	if entry := s.lookupEntry(name); entry != nil {
		e, ok := entry.ref.(dag.Expr)
		if !ok {
			return nil, fmt.Errorf("symbol %q is not bound to an expression", name)
		}
		return e, nil
	}
	return nil, nil
}

func (s *Scope) lookupOp(name string) (*opDecl, error) {
	if entry := s.lookupEntry(name); entry != nil {
		d, ok := entry.ref.(*opDecl)
		if !ok {
			return nil, fmt.Errorf("symbol %q is not bound to an operator", name)
		}
		return d, nil
	}
	return nil, nil
}

func (s *Scope) lookupEntry(name string) *entry {
	for scope := s; scope != nil; scope = scope.parent {
		if entry, ok := scope.symbols[name]; ok {
			return entry
		}
	}
	return nil
}

func (s *Scope) nvars() int {
	var n int
	for scope := s; scope != nil; scope = scope.parent {
		n += scope.nvar
	}
	return n
}

// resolve paths based on SQL semantics in order of precedence
// and replace with dag path with schemafied semantics.
// In the case of unqualified col ref, check that it is not ambiguous
// when there are multiple tables (i.e., from joins).
// An unqualified field reference is valid only in dynamic schemas.
func (s *Scope) resolve(path field.Path) (field.Path, error) {
	// If there's no schema, we're not in a SQL context so we just
	// return the path unmodified.  Otherwise, we apply SQL scoping
	// rules to transform the abstract path to the dataflow path
	// implied by the schema.
	if s.schema == nil {
		return path, nil
	}
	if len(path) == 0 {
		// XXX this should really treat this as a column in sql context but
		// but this will cause dynamic stuff to silently fail so I think we
		// should flag and maybe make it part of a strict mode (like bitwise |)
		return nil, errors.New("cannot reference 'this' in relational context; consider the 'yield' operator")
	}
	if len(path) == 1 {
		return resolveColumn(s.schema, path[0], nil)
	}
	if out, err := resolveTable(s.schema, path[0], path[1:]); out != nil || err != nil {
		return out, err
	}
	out, err := resolveColumn(s.schema, path[0], path[1:])
	if out == nil && err == nil {
		err = fmt.Errorf("%q: not a column or table", path[0])
	}
	return out, err
}

func resolveTable(schema schema, table string, path field.Path) (field.Path, error) {
	switch schema := schema.(type) {
	case *schemaDynamic:
		if strings.EqualFold(schema.name, table) {
			return path, nil
		}
	case *schemaStatic:
		if strings.EqualFold(schema.name, table) {
			if len(path) == 0 {
				return []string{}, nil
			}
			return resolveColumn(schema, path[0], path[1:])
		}
	case *schemaSelect:
		if schema.out != nil {
			target, err := resolveTable(schema.out, table, path)
			if err != nil {
				return nil, err
			}
			if target != nil {
				return append([]string{"out"}, target...), nil
			}
		}
		target, err := resolveTable(schema.in, table, path)
		if err != nil {
			return nil, err
		}
		if target != nil {
			return append([]string{"in"}, target...), nil
		}

	case *schemaJoin:
		out, err := resolveTable(schema.left, table, path)
		if err != nil {
			return nil, err
		}
		if out != nil {
			chk, err := resolveTable(schema.right, table, path)
			if err != nil {
				return nil, err
			}
			if chk != nil {
				return nil, fmt.Errorf("%q: ambiguous table reference", table)
			}
			return append([]string{"left"}, out...), nil
		}
		out, err = resolveTable(schema.right, table, path)
		if err != nil {
			return nil, err
		}
		if out != nil {
			return append([]string{"right"}, out...), nil
		}
	}
	return nil, nil
}

func resolveColumn(schema schema, col string, path field.Path) (field.Path, error) {
	switch schema := schema.(type) {
	case *schemaDynamic:
		return append([]string{col}, path...), nil
	case *schemaStatic:
		for _, c := range schema.columns {
			if c == col {
				return append([]string{col}, path...), nil
			}
		}
	case *schemaAnon:
		for _, c := range schema.columns {
			if c == col {
				return append([]string{col}, path...), nil
			}
		}
	case *schemaSelect:
		if schema.out != nil {
			resolved, err := resolveColumn(schema.out, col, path)
			if err != nil {
				return nil, err
			}
			if resolved != nil {
				return append([]string{"out"}, resolved...), nil
			}
		}
		resolved, err := resolveColumn(schema.in, col, path)
		if err != nil {
			return nil, err
		}
		if resolved != nil {
			return append([]string{"in"}, resolved...), nil
		}
	case *schemaJoin:
		out, err := resolveColumn(schema.left, col, path)
		if err != nil {
			return nil, err
		}
		if out != nil {
			chk, err := resolveColumn(schema.right, col, path)
			if err != nil {
				return nil, err
			}
			if chk != nil {
				return nil, fmt.Errorf("%q: ambiguous column reference", col)
			}
			return append([]string{"left"}, out...), nil
		}
		out, err = resolveColumn(schema.right, col, path)
		if err != nil {
			return nil, err
		}
		if out != nil {
			return append([]string{"right"}, out...), nil
		}
	}
	return nil, nil
}

// derefSchema adds logic to seq to yield out the value from a SQL-schema-contained
// value set and returns the resulting schema.  If name is non-zero, then a new
// schema is returned that represents the aliased table name that results.
func derefSchema(sch schema, seq dag.Seq, name string) (dag.Seq, schema) {
	switch sch := sch.(type) {
	case *schemaDynamic:
		if name != "" {
			sch = &schemaDynamic{name: name}
		}
		return seq, sch
	case *schemaStatic:
		if name != "" {
			sch = &schemaStatic{name: name, columns: sch.columns}
		}
		return seq, sch
	case *schemaAnon:
		return seq, sch
	case *schemaSelect:
		if name == "" {
			// postgres and duckdb oddly do this
			name = "unamed_subquery"
		}
		var outSchema schema
		if anon, ok := sch.out.(*schemaAnon); ok {
			// Hide any enclosing schema hierarchy by just exporting the
			// select columns.
			outSchema = &schemaStatic{name: name, columns: anon.columns}
		} else {
			// This is a select value.
			// XXX we should eventually have a way to propagate schema info here,
			// e.g., record expression with fixed columns as an anonSchema.
			outSchema = &schemaDynamic{}
		}
		return append(seq, &dag.Yield{
			Kind:  "Yield",
			Exprs: []dag.Expr{pathOf("out")},
		}), outSchema
	case *schemaJoin:
		return append(seq, &dag.Yield{
			Kind:  "Yield",
			Exprs: []dag.Expr{joinSpread(nil, nil)},
		}), &schemaDynamic{name: name}
	default:
		panic(fmt.Sprintf("unknown schema type: %T", sch))
	}
}

func derefThis(sch schema, path []string) dag.Expr {
	switch sch := sch.(type) {
	case *schemaDynamic, *schemaStatic, *schemaAnon:
		return &dag.This{Kind: "This", Path: path}
	case *schemaSelect:
		return derefThis(sch.in, append(path, "in"))
	case *schemaJoin:
		left := derefThis(sch.left, append(path, "left"))
		right := derefThis(sch.right, append(path, "right"))
		return joinSpread(left, right)
	default:
		panic(fmt.Sprintf("unknown schema type: %T", sch))
	}
}

// spread left/right join legs into "this"
func joinSpread(left, right dag.Expr) *dag.RecordExpr {
	if left == nil {
		left = &dag.This{Kind: "This"}
	}
	if right == nil {
		right = &dag.This{Kind: "This"}
	}
	return &dag.RecordExpr{
		Kind: "RecordExpr",
		Elems: []dag.RecordElem{
			&dag.Spread{
				Kind: "Spread",
				Expr: left,
			},
			&dag.Spread{
				Kind: "Spread",
				Expr: right,
			},
		},
	}
}
