package semantic

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/compiler/rungen"
	"github.com/brimdata/super/compiler/semantic/sem"
	"github.com/brimdata/super/sup"
)

type evaluator struct {
	reporter reporter
	in       map[string]*sem.FuncDef
	errs     []errloc
}

type errloc struct {
	loc ast.Node
	err error
}

func newEvaluator(r reporter, funcs map[string]*sem.FuncDef) *evaluator {
	return &evaluator{
		reporter: r,
		in:       funcs,
	}
}

func (e *evaluator) mustEval(sctx *super.Context, expr sem.Expr) (super.Value, bool) {
	val, ok := e.maybeEval(sctx, expr)
	e.flushErrs()
	return val, ok
}

func (e *evaluator) maybeEval(sctx *super.Context, expr sem.Expr) (super.Value, bool) {
	if literal, ok := expr.(*sem.LiteralExpr); ok {
		val, err := sup.ParseValue(sctx, literal.Value)
		if err != nil {
			e.error(literal.Node, err)
			return val, false
		}
		return val, true
	}
	// re-enter the semantic analyzer with just this expr by resolving
	// all needed funcs then traversing the resulting sem tree and seeing
	// if will eval as a compile-time constant.  If so, compile it the rest
	// of the way and invoke rungen to get the result and return it.
	// If an error is encountered, returns the error.  If the expression
	// isn't a compile-time const, then return ErrNotConst XXX.  Note that
	// no existing state in the translator is touched nor is the passed in
	// sem tree modified at all; instead, the process here creates copies
	// of any needed sem tree and funcs.

	//XXX share this stuff with main analyzer so that when we add type checking etc
	// it all evolves together

	r := newResolver(e.reporter, e.in)
	resolvedExpr, funcs := r.resolveExpr(expr)
	e.expr(resolvedExpr)
	if len(e.errs) > 0 {
		return super.Value{}, false
	}
	main := newDagen(e.reporter).assembleExpr(resolvedExpr, funcs)
	val, err := rungen.EvalAtCompileTime(sctx, main)
	if err != nil {
		e.error(nil, err) //XXX fix nil
		return val, false
	}
	return val, true
}

func (e *evaluator) seq(seq sem.Seq) {
	for _, op := range seq {
		e.op(op)
	}
}

func (e *evaluator) op(op sem.Op) {
	switch op := op.(type) {
	case *sem.AggregateOp:
		e.assignments(op.Keys)
		e.assignments(op.Aggs)
	case *sem.BadOp:
	case *sem.ForkOp:
		for _, seq := range op.Paths {
			e.seq(seq)
		}
	case *sem.SwitchOp:
		for _, c := range op.Cases {
			e.expr(c.Expr)
			e.seq(c.Path)
		}
	case *sem.SortOp:
		e.sortExprs(op.Exprs)
	case *sem.CutOp:
		e.assignments(op.Args)
	case *sem.DebugOp:
		e.expr(op.Expr)
	case *sem.DistinctOp:
		e.expr(op.Expr)
	case *sem.DropOp:
		e.exprs(op.Args)
	case *sem.HeadOp:
	case *sem.TailOp:
	case *sem.SkipOp:
	case *sem.FilterOp:
		e.expr(op.Expr)
	case *sem.UniqOp:
	case *sem.TopOp:
		e.sortExprs(op.Exprs)
	case *sem.PutOp:
		e.assignments(op.Args)
	case *sem.RenameOp:
		e.assignments(op.Args)
	case *sem.FuseOp:
	case *sem.JoinOp:
		e.expr(op.Cond)
	case *sem.ExplodeOp:
		e.exprs(op.Args)
	case *sem.UnnestOp:
		e.expr(op.Expr)
		e.seq(op.Body)
	case *sem.ValuesOp:
		e.exprs(op.Exprs)
	case *sem.MergeOp:
		e.sortExprs(op.Exprs)
	case *sem.LoadOp:
	case *sem.OutputOp:
	case *sem.DefaultScan:
	case *sem.FileScan:
		//XXX error here
	case *sem.HTTPScan:
		//XXX error here
	case *sem.PoolScan:
		//XXX error here
	case *sem.RobotScan:
		//XXX error here
	case *sem.DBMetaScan:
		//XXX error here
	case *sem.PoolMetaScan:
		//XXX error here
	case *sem.CommitMetaScan:
		//XXX error here
	case *sem.NullScan:
	case *sem.DeleteScan:
		//XXX error here
	}
}

func (e *evaluator) assignments(assignments []sem.Assignment) {
	for _, a := range assignments {
		e.expr(a.LHS)
		e.expr(a.RHS)
	}
}

func (e *evaluator) sortExprs(exprs []sem.SortExpr) {
	for _, se := range exprs {
		e.expr(se.Expr)
	}
}

func (e *evaluator) exprs(exprs []sem.Expr) {
	for _, expr := range exprs {
		e.expr(expr)
	}
}

func (e *evaluator) expr(expr sem.Expr) {
	switch expr := expr.(type) {
	case nil:
	case *sem.AggFunc:
		e.expr(expr.Expr)
		e.expr(expr.Where)
	case *sem.ArrayExpr:
		e.arrayElems(expr.Elems)
	case *sem.BadExpr:
	case *sem.BinaryExpr:
		e.expr(expr.LHS)
		e.expr(expr.RHS)
	case *sem.CallExpr:
		//XXX need to look at call to see if it has side effects?
		// like now()?  or is now() ok?
		e.exprs(expr.Args)
	case *sem.CondExpr:
		e.expr(expr.Cond)
		e.expr(expr.Then)
		e.expr(expr.Else)
	case *sem.DotExpr:
		e.expr(expr.LHS)
	case *sem.IndexExpr:
		e.expr(expr.Expr)
		e.expr(expr.Index)
	case *sem.IsNullExpr:
		e.expr(expr.Expr)
	case *sem.LiteralExpr:
	case *sem.MapCallExpr:
		e.expr(expr.Expr)
		e.expr(expr.Lambda)
	case *sem.MapExpr:
		for _, entry := range expr.Entries {
			e.expr(entry.Key)
			e.expr(entry.Value)
		}
	case *sem.RecordExpr:
		e.recordElems(expr.Elems)
	case *sem.RegexpMatchExpr:
		e.expr(expr.Expr)
	case *sem.RegexpSearchExpr:
		e.expr(expr.Expr)
	case *sem.SearchTermExpr:
		e.expr(expr.Expr)
	case *sem.SetExpr:
		e.arrayElems(expr.Elems)
	case *sem.SliceExpr:
		e.expr(expr.Expr)
		e.expr(expr.From)
		e.expr(expr.To)
	case *sem.SubqueryExpr:
		e.seq(expr.Body)
	case *sem.ThisExpr:
		//XXX error here
	case *sem.UnaryExpr:
		e.expr(expr.Operand)
	}
}

func (e *evaluator) arrayElems(elems []sem.ArrayElem) {
	for _, elem := range elems {
		switch elem := elem.(type) {
		case *sem.SpreadElem:
			e.expr(elem.Expr)
		case *sem.ExprElem:
			e.expr(elem.Expr)
		default:
			panic(elem)
		}
	}
}

func (e *evaluator) recordElems(elems []sem.RecordElem) {
	for _, elem := range elems {
		switch elem := elem.(type) {
		case *sem.SpreadElem:
			e.expr(elem.Expr)
		case *sem.FieldElem:
			e.expr(elem.Value)
		default:
			panic(elem)
		}
	}
}

func (e *evaluator) error(loc ast.Node, err error) {
	e.errs = append(e.errs, errloc{loc, err})
}

func (e *evaluator) flushErrs() {
	for _, info := range e.errs {
		if info.loc == nil { //XXX
			e.reporter.errorNoLoc(info.err)
		}
		e.reporter.error(info.loc, info.err)
	}
}
