package semantic

import (
	"context"
	"errors"
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
	r := reporter{p.Files()}
	t := newTranslator(ctx, r, env)
	astseq := p.Parsed()
	if extInput {
		astseq.Prepend(&ast.DefaultScan{Kind: "DefaultScan"})
	}
	seq := t.semSeq(astseq)
	if err := r.Error(); err != nil {
		return nil, err
	}
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
	return resolveAndGen(r, seq, t.funcsByTag)
}

func resolveAndGen(reporter reporter, seq sem.Seq, funcs map[string]*sem.FuncDef) (*dag.Main, error) {
	r := newResolver(reporter, funcs)
	semSeq, dagFuncs := r.resolve(seq)
	if err := reporter.Error(); err != nil {
		return nil, err
	}
	main := newDagen(reporter).assemble(semSeq, dagFuncs)
	return main, r.Error()
}

func resolveAndGenExpr(reporter reporter, expr sem.Expr, funcs map[string]*sem.FuncDef) (*dag.MainExpr, error) {
	r := newResolver(reporter, funcs)
	semExpr, dagFuncs := r.resolveExpr(expr)
	main := newDagen(reporter).assembleExpr(semExpr, dagFuncs)
	return main, r.Error()
}

// Translate AST into semantic tree.  Resolve all bindings
// between symbols and their entities and flatten all scopes
// creating a global function table.  Convert SQL entities
// to dataflow.
type translator struct {
	reporter
	ctx         context.Context
	opStack     []*ast.OpDecl
	cteStack    []*cte
	env         *exec.Environment
	scope       *Scope
	sctx        *super.Context
	funcsByTag  map[string]*sem.FuncDef
	funcsByDecl map[*ast.Decl]*sem.FuncDef
}

func newTranslator(ctx context.Context, r reporter, env *exec.Environment) *translator {
	return &translator{
		reporter:    r,
		ctx:         ctx,
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
	case *sem.ForkOp:
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
		Node:   body,
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

type reporter struct {
	*srcfiles.List
}

func (r reporter) error(n ast.Node, err error) {
	r.AddError(err.Error(), n.Pos(), n.End())
}

// We should get rid of this and make sure tracking refs follow
// everything that needs to report errors.
func (r reporter) errorNoLoc(err error) {
	r.AddError(err.Error(), -1, -1)
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
