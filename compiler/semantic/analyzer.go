package semantic

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/brimdata/super"
	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/compiler/dag"
	"github.com/brimdata/super/compiler/parser"
	"github.com/brimdata/super/compiler/semantic/sem"
	"github.com/brimdata/super/compiler/srcfiles"
	"github.com/brimdata/super/runtime/exec"
)

// Motivation: to do type checking, we need functions unraveled (or at least the
// bindings simplified so we can unravel on the fly in recursion)
// Challenge: if we resolve SQL to dataflow AST, then do type checks on
// this AST, we need to map errors back to original SQL expressions.
//
// New breakdown:
// Pass 1: convert SQL to dataflow AST, resolve symbols with scope
// by flattening scopes (cte refs, function refs, and so forth)
//  insert all implied sources
// (after phase 1, scopes are all resolved and no longer needed)
//
// Invariant: all AST mods keep pointers to original source locs so error reporting
// remains good all the way up to the DAG gen

// Analyze performs a semantic analysis of the AST, translating it from AST
// to DAG form, resolving syntax ambiguities, and performing constant propagation.
// After semantic analysis, the DAG is ready for either optimization or compilation.
func Analyze(ctx context.Context, p *parser.AST, env *exec.Environment, extInput bool) (*dag.Main, error) {
	files := p.Files()
	t := newTranslator(ctx, files, env)
	astseq := p.Parsed()
	if extInput {
		astseq.Prepend(&ast.DefaultScan{Kind: "DefaultScan"})
	}
	seq := t.semSeq(astseq)
	if !HasSource(seq) {
		if t.env.IsAttached() {
			if len(seq) == 0 {
				return nil, errors.New("query text is missing")
			}
			seq.Prepend(&sem.NullScan{})
		} else {
			// This is a local query and there's no external input
			// (i.e., no command-line file args)
			seq.Prepend(&sem.NullScan{})
		}
	}
	//XXX need daggen
	seq = a.checkOutputs(true, seq)
	r := newResolver(a)
	seq, funcs := r.resolve(seq)
	// Sort function entries so they are consistently ordered by integer tag strings.
	slices.SortFunc(funcs, func(a, b *dag.FuncDef) int {
		return strings.Compare(a.Tag, b.Tag)
	})
	return &dag.Main{Funcs: funcs, Body: seq}, files.Error()
}

// Translate AST into semantic tree.  Resolve all bindings
// between symbols and their entities and flatten all scopes
// creating a global function table.  Convert SQL entities
// to dataflow.
type translator struct {
	ctx         context.Context
	files       *srcfiles.List
	opStack     []*ast.OpDecl
	cteStack    []*cte
	env         *exec.Environment
	scope       *Scope
	sctx        *super.Context
	funcsByTag  map[string]*sem.FuncDef
	funcsByDecl map[*ast.Decl]*sem.FuncDef
}

func newTranslator(ctx context.Context, files *srcfiles.List, env *exec.Environment) *translator {
	return &translator{
		ctx:         ctx,
		files:       files,
		outputs:     make(map[*dag.Output]ast.Node),
		env:         env,
		scope:       NewScope(nil),
		sctx:        super.NewContext(),
		funcsByTag:  make(map[string]*sem.FuncDef),
		funcsByDecl: make(map[*ast.Decl]*sem.FuncDef),
	}
}

func HasSource(seq sem.Seq) bool {
	if len(seq) == 0 {
		return false
	}
	switch op := seq[0].(type) {
	case *sem.FileScan, *sem.HTTPScan, *sem.PoolScan, *sem.DBMetaScan, *sem.PoolMetaScan, *sem.CommitMetaScan, *sem.DeleteScan, *sem.NullScan:
		return true
	case *dag.Fork:
		for _, path := range op.Paths {
			if !HasSource(path) {
				return false
			}
		}
		return true
	}
	return false
}

func (t *translator) enterScope() {
	t.scope = NewScope(t.scope)
}

func (t *translator) exitScope() {
	t.scope = t.scope.parent
}

func (t *translator) newFunc(body ast.Expr, name string, params []string, e sem.Expr) string {
	tag := strconv.Itoa(len(t.funcsByTag))
	t.funcsByTag[tag] = &sem.FuncDef{
		AST:    body,
		Tag:    tag,
		Name:   name,
		Params: params,
		Body:   e,
	}
	return tag
}

type opDecl struct {
	ast   *ast.OpDecl
	scope *Scope // parent scope of op declaration.
	bad   bool
}

type opCycleError []*ast.OpDecl

func (e opCycleError) Error() string {
	b := "operator cycle found: "
	for i, op := range e {
		if i > 0 {
			b += " -> "
		}
		b += op.Name.Name
	}
	return b
}

func badExpr() sem.Expr {
	return &sem.BadExpr{}
}

func badOp() sem.Op {
	return &sem.BadOp{}
}

func (t *translator) error(n ast.Node, err error) {
	t.files.AddError(err.Error(), n.Pos(), n.End())
}

// XXX this is for gendag XXX this is messed up
// XXX this method should live on daggen
func (t *translator) checkOutputs(isLeaf bool, seq dag.Seq) dag.Seq {
	if len(seq) == 0 {
		return seq
	}
	// - Report an error in any outputs are not located in the leaves.
	// - Add output operators to any leaves where they do not exist.
	lastN := len(seq) - 1
	for i, o := range seq {
		isLast := lastN == i
		switch o := o.(type) {
		case *dag.Output:
			if !isLast || !isLeaf {
				n, ok := t.outputs[o]
				if !ok {
					panic("system error: untracked user output")
				}
				t.error(n, errors.New("output operator must be at flowgraph leaf"))
			}
		case *dag.Scatter:
			for k := range o.Paths {
				o.Paths[k] = t.checkOutputs(isLast && isLeaf, o.Paths[k])
			}
		case *dag.Unnest:
			o.Body = t.checkOutputs(false, o.Body)
		case *dag.Fork:
			for k := range o.Paths {
				o.Paths[k] = t.checkOutputs(isLast && isLeaf, o.Paths[k])
			}
		case *dag.Switch:
			for k := range o.Cases {
				o.Cases[k].Path = t.checkOutputs(isLast && isLeaf, o.Cases[k].Path)
			}
		case *dag.Mirror:
			o.Main = t.checkOutputs(isLast && isLeaf, o.Main)
			o.Mirror = t.checkOutputs(isLast && isLeaf, o.Mirror)
		}
	}
	switch seq[lastN].(type) {
	case *dag.Output, *dag.Scatter, *dag.Fork, *dag.Switch, *dag.Mirror:
	default:
		if isLeaf {
			return append(seq, &dag.Output{Name: "main"})
		}
	}
	return seq
}
