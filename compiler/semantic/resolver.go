package semantic

import (
	"fmt"
	"strconv"

	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/compiler/semantic/sem"
	"github.com/brimdata/super/runtime/sam/expr/function"
)

type resolver struct {
	translator *translator
	in         map[string]*sem.FuncDef
	fixed      map[string]*sem.FuncDef
	variants   []*sem.FuncDef
	params     []map[string]string
	ntag       int
}

func newResolver(t *translator) *resolver {
	r := &resolver{
		translator: t,
		in:         t.funcsByTag,
		fixed:      make(map[string]*sem.FuncDef),
	}
	return r
}

func (r *resolver) resolve(seq sem.Seq) (sem.Seq, []*sem.FuncDef) {
	out := r.resolveSeq(seq)
	funcs := r.variants
	for _, f := range r.fixed {
		funcs = append(funcs, f)
	}
	return out, funcs
}

// resolveSeq traverses a DAG and substitues all instances of CallParam by rewriting
// each called function that incudes one or more CallParams substituting the actual
// function called (which is also possibly resolved) as indicated by a FuncRef.
// Each function is also rewritten to remove the function parameters from its params.
// After resolve is done, all CallParam or FuncRef pseudo-expression nodes are
// removed from the sem.  All functions are converted from the input function table
// to the output function table whether or not they needed to be resolved.
// Any FuncRefs that do not get bound to a CallParam (i.e., appear in random expressions)
// are found and reported as error as are any CallParam that are called with non-FuncRef
// arguments.
func (r *resolver) resolveSeq(seq sem.Seq) sem.Seq {
	var out sem.Seq
	for _, op := range seq {
		out = append(out, r.resolveOp(op))
	}
	return out
}

func (r *resolver) resolveOp(op sem.Op) sem.Op {
	switch op := op.(type) {
	case *sem.AggregateOp:
		return &sem.AggregateOp{
			AST:   op.AST,
			Limit: op.Limit,
			Keys:  r.resolveAssignments(op.Keys),
			Aggs:  r.resolveAssignments(op.Aggs),
		}
	case *sem.BadOp:
	case *sem.ForkOp:
		var paths []sem.Seq
		for _, seq := range op.Paths {
			paths = append(paths, r.resolveSeq(seq))
		}
		return &sem.ForkOp{
			Paths: paths,
		}
	case *sem.SwitchOp:
		var cases []sem.Case
		for _, c := range op.Cases {
			cases = append(cases, sem.Case{
				Expr: r.resolveExpr(c.Expr),
				Path: r.resolveSeq(c.Path),
			})
		}
		return &sem.SwitchOp{
			AST:   op.AST,
			Expr:  r.resolveExpr(op.Expr),
			Cases: cases,
		}
	case *sem.SortOp:
		return &sem.SortOp{
			AST:   op.AST,
			Exprs: r.resolveSortExprs(op.Exprs),
		}
	case *sem.CutOp:
		return &sem.CutOp{
			AST:  op.AST,
			Args: r.resolveAssignments(op.Args),
		}
	case *sem.DebugOp:
		return &sem.DebugOp{
			AST:  op.AST,
			Expr: r.resolveExpr(op.Expr),
		}
	case *sem.DistinctOp:
		return &sem.DistinctOp{
			AST:  op.AST,
			Expr: r.resolveExpr(op.Expr),
		}
	case *sem.DropOp:
		return &sem.DropOp{
			AST:  op.AST,
			Args: r.resolveExprs(op.Args),
		}
	case *sem.HeadOp:
	case *sem.TailOp:
	case *sem.SkipOp:
	case *sem.FilterOp:
		return &sem.FilterOp{
			AST:  op.AST,
			Expr: r.resolveExpr(op.Expr),
		}
	case *sem.UniqOp:
	case *sem.TopOp:
		return &sem.TopOp{
			AST:   op.AST,
			Limit: op.Limit,
			Exprs: r.resolveSortExprs(op.Exprs),
		}
	case *sem.PutOp:
		return &sem.PutOp{
			AST:  op.AST,
			Args: r.resolveAssignments(op.Args),
		}
	case *sem.RenameOp:
		return &sem.RenameOp{
			AST:  op.AST,
			Args: r.resolveAssignments(op.Args),
		}
	case *sem.FuseOp:
	case *sem.JoinOp:
		return &sem.JoinOp{
			AST:        op.AST,
			Style:      op.Style,
			LeftAlias:  op.LeftAlias,
			RightAlias: op.RightAlias,
			Cond:       r.resolveExpr(op.Cond),
		}
	case *sem.ExplodeOp:
		return &sem.ExplodeOp{
			AST:  op.AST,
			Args: r.resolveExprs(op.Args),
			Type: op.Type,
			As:   op.As,
		}
	case *sem.UnnestOp:
		return &sem.UnnestOp{
			AST:  op.AST,
			Expr: r.resolveExpr(op.Expr),
			Body: r.resolveSeq(op.Body),
		}
	case *sem.ValuesOp:
		return &sem.ValuesOp{
			AST:   op.AST,
			Exprs: r.resolveExprs(op.Exprs),
		}
	case *sem.MergeOp:
		return &sem.MergeOp{
			AST:   op.AST,
			Exprs: r.resolveSortExprs(op.Exprs),
		}

	case *sem.LoadOp:
	case *sem.OutputOp:
	case *sem.DefaultScan:
	case *sem.FileScan:
	case *sem.HTTPScan:
	case *sem.PoolScan:
	case *sem.RobotScan:
		return &sem.RobotScan{
			AST:    op.AST,
			Expr:   r.resolveExpr(op.Expr),
			Format: op.Format,
		}
	case *sem.DBMetaScan:
	case *sem.PoolMetaScan:
	case *sem.CommitMetaScan:
	case *sem.NullScan:
	case *sem.DeleteScan:
		return &sem.DeleteScan{
			AST:   op.AST,
			Where: r.resolveExpr(op.Where),
		}
	default:
		panic(op)
	}
	return op
}

func (r *resolver) resolveAssignments(assignments []sem.Assignment) []sem.Assignment {
	var out []sem.Assignment
	for _, ass := range assignments {
		out = append(out, sem.Assignment{
			AST: ass.AST,
			LHS: r.resolveExpr(ass.LHS),
			RHS: r.resolveExpr(ass.RHS),
		})
	}
	return out
}

func (r *resolver) resolveSortExprs(exprs []sem.SortExpr) []sem.SortExpr {
	var out []sem.SortExpr
	for _, e := range exprs {
		out = append(out, sem.SortExpr{
			AST:   e.AST,
			Expr:  r.resolveExpr(e.Expr),
			Order: e.Order,
			Nulls: e.Nulls})
	}
	return out
}

func (r *resolver) resolveExprs(exprs []sem.Expr) []sem.Expr {
	var out []sem.Expr
	for _, e := range exprs {
		out = append(out, r.resolveExpr(e))
	}
	return out
}

func (r *resolver) resolveExpr(e sem.Expr) sem.Expr {
	switch e := e.(type) {
	case nil:
		return nil
	case *sem.FuncRef:
		// This needs to be in an argument list and can't be anywhere else... bad DAG
		panic(e)
	case *sem.CallParam:
		// This is a call to a parameter.  It must only appear enclosed in a FuncDef
		// with params containing the name in the e.Param.  The function being referenced
		// or passed in can be lazily created.
		return r.resolveCallParam(e)
	case *sem.AggFunc:
		return &sem.AggFunc{
			AST:      e.AST,
			Name:     e.Name,
			Distinct: e.Distinct,
			Expr:     r.resolveExpr(e.Expr),
			Where:    r.resolveExpr(e.Where),
		}
	case *sem.ArrayExpr:
		return &sem.ArrayExpr{
			AST:   e.AST,
			Elems: r.resolveVectorElems(e.Elems),
		}
	case *sem.BadExpr:
	case *sem.BinaryExpr:
		return sem.NewBinaryExpr(e.AST, e.Op, r.resolveExpr(e.LHS), r.resolveExpr(e.RHS))
	case *sem.CallExpr:
		return r.resolveCall(e.AST, e.Tag, e.Args)
	case *sem.CondExpr:
		return &sem.CondExpr{
			AST:  e.AST,
			Cond: r.resolveExpr(e.Cond),
			Then: r.resolveExpr(e.Then),
			Else: r.resolveExpr(e.Else),
		}
	case *sem.DotExpr:
		return &sem.DotExpr{
			AST: e.AST,
			LHS: r.resolveExpr(e.LHS),
			RHS: e.RHS,
		}
	case *sem.IndexExpr:
		return &sem.IndexExpr{
			AST:   e.AST,
			Expr:  r.resolveExpr(e.Expr),
			Index: r.resolveExpr(e.Index),
		}
	case *sem.IsNullExpr:
		return &sem.IsNullExpr{
			AST:  e.AST,
			Expr: r.resolveExpr(e.Expr),
		}
	case *sem.LiteralExpr:
	case *sem.MapCallExpr:
		call, ok := r.resolveCall(e.AST, e.Lambda.Tag, e.Lambda.Args).(*sem.CallExpr)
		if !ok {
			return badExpr()
		}
		return &sem.MapCallExpr{
			AST:    e.AST,
			Expr:   r.resolveExpr(e.Expr),
			Lambda: call,
		}
	case *sem.MapExpr:
		var entries []sem.Entry
		for _, entry := range e.Entries {
			entries = append(entries, sem.Entry{
				Key:   r.resolveExpr(entry.Key),
				Value: r.resolveExpr(entry.Value),
			})
		}
		return &sem.MapExpr{
			AST:     e.AST,
			Entries: entries,
		}
	case *sem.RecordExpr:
		return &sem.RecordExpr{
			AST:   e.AST,
			Elems: r.resolveRecordElems(e.Elems),
		}
	case *sem.RegexpMatchExpr:
		return &sem.RegexpMatchExpr{
			AST:     e.AST,
			Pattern: e.Pattern,
			Expr:    r.resolveExpr(e.Expr),
		}
	case *sem.RegexpSearchExpr:
		return &sem.RegexpSearchExpr{
			AST:     e.AST,
			Pattern: e.Pattern,
			Expr:    r.resolveExpr(e.Expr),
		}
	case *sem.SearchTermExpr:
		return &sem.SearchTermExpr{
			AST:   e.AST,
			Text:  e.Text,
			Value: e.Value,
			Expr:  r.resolveExpr(e.Expr),
		}
	case *sem.SetExpr:
		return &sem.SetExpr{
			AST:   e.AST,
			Elems: r.resolveVectorElems(e.Elems),
		}
	case *sem.SliceExpr:
		return &sem.SliceExpr{
			AST:  e.AST,
			Expr: r.resolveExpr(e.Expr),
			From: r.resolveExpr(e.From),
			To:   r.resolveExpr(e.To),
		}
	case *sem.SubqueryExpr:
		// We clear params before processing a subquery so you can't
		// touch passed-in functions inside the first "this" of a correlated
		// subquery.  We can support this later if people are interested.
		// It requires a bit of surgery.
		r.pushParams(make(map[string]string))
		defer r.popParams()
		return &sem.SubqueryExpr{
			AST:        e.AST,
			Correlated: e.Correlated,
			Body:       r.resolveSeq(e.Body),
		}
	case *sem.ThisExpr:
	case *sem.UnaryExpr:
		return sem.NewUnaryExpr(e.AST, e.Op, r.resolveExpr(e.Operand))
	default:
		panic(e)
	}
	return e
}

func (r *resolver) resolveVectorElems(elems []sem.ArrayElem) []sem.ArrayElem {
	var out []sem.ArrayElem
	for _, elem := range elems {
		switch elem := elem.(type) {
		case *sem.SpreadElem:
			out = append(out, &sem.SpreadElem{
				AST:  elem.AST,
				Expr: r.resolveExpr(elem.Expr),
			})
		default:
			out = append(out, r.resolveExpr(elem.(sem.Expr)).(sem.ArrayElem))
		}
	}
	return out
}

func (r *resolver) resolveRecordElems(elems []sem.RecordElem) []sem.RecordElem {
	var out []sem.RecordElem
	for _, elem := range elems {
		switch elem := elem.(type) {
		case *sem.SpreadElem:
			out = append(out, &sem.SpreadElem{
				AST:  elem.AST,
				Expr: r.resolveExpr(elem.Expr),
			})
		case *sem.FieldElem:
			out = append(out, &sem.FieldElem{
				AST:   elem.AST,
				Name:  elem.Name,
				Value: r.resolveExpr(elem.Value),
			})
		default:
			panic(elem)
		}
	}
	return out
}

func (r *resolver) error(loc ast.Loc, err error) {
	r.translator.error(loc, err)
}

func (r *resolver) resolveCallParam(call *sem.CallParam) sem.Expr {
	oldTag := r.lookupParam(call.Param)
	if oldTag == "" {
		// This can happen when we go to resolve a parameter that wasn't bound to
		// an actual function because some other value was bound to it so it didn't
		// get put in the parameter table.
		r.error(call.AST.Loc, fmt.Errorf("function called via parameter %q is bound to a non-function", call.Param))
		return badExpr()
	}
	if isBuiltin(oldTag) {
		// Check argument count here for builtin functions.
		if _, err := function.New(r.translator.sctx, oldTag, len(call.Args)); err != nil {
			r.error(call.AST.Loc, fmt.Errorf("function %q called via parameter %q: %w", oldTag, call.Param, err))
			return badExpr()
		}
	}
	return r.resolveCall(call.AST, oldTag, call.Args)
}

func (r *resolver) resolveCall(callAST ast.Expr, oldTag string, args []sem.Expr) sem.Expr {
	if isBuiltin(oldTag) {
		return &sem.CallExpr{
			AST:  callAST,
			Tag:  oldTag,
			Args: r.resolveExprs(args),
		}
	}
	// Translate the tag to the new func table and convert any
	// function refs passed as args to lookup-table removing
	// correponding args.
	var params []string
	var exprs []sem.Expr
	funcDef := r.in[oldTag]
	if len(funcDef.Params) != len(args) {
		loc, _ := callAST.(ast.Node)
		r.error(loc, fmt.Errorf("%q: expected %d params but called with %d", funcDef.Name, len(funcDef.Params), len(args)))
		return badExpr()
	}
	bindings := make(map[string]string)
	for k, arg := range args {
		if f, ok := arg.(*sem.FuncRef); ok {
			bindings[funcDef.Params[k]] = f.Tag
			continue
		}
		e := r.resolveExpr(arg)
		if e, ok := e.(*sem.ThisExpr); ok {
			if len(e.Path) == 1 {
				// Propagate a function passed as a function value inside of
				// a function to another function.
				if tag := r.lookupParam(e.Path[0]); tag != "" {
					bindings[funcDef.Params[k]] = tag
					continue
				}
			}
		}
		params = append(params, funcDef.Params[k])
		exprs = append(exprs, r.resolveExpr(arg))
	}
	if len(funcDef.Params) == len(params) {
		// No need to specialize this call since no function args are being passed.
		newTag := r.lookupFixed(oldTag)
		return &sem.CallExpr{
			AST:  callAST,
			Tag:  newTag,
			Args: args,
		}
	}
	// Enter the new function scope and set up the bindings for the
	// values we retrieved above while evaluating args in the outer scope.
	r.pushParams(bindings)
	defer r.popParams()
	newTag := r.lookupVariant(oldTag, params)
	return &sem.CallExpr{
		AST:  callAST,
		Tag:  newTag,
		Args: exprs,
	}
}

func (r *resolver) lookupFixed(oldTag string) string {
	if funcDef, ok := r.fixed[oldTag]; ok {
		return funcDef.Tag
	}
	funcDef := r.in[oldTag]
	newTag := r.nextTag()
	newFuncDef := &sem.FuncDef{
		AST:    funcDef.AST,
		Tag:    newTag,
		Name:   funcDef.Name,
		Params: funcDef.Params,
	}
	r.fixed[oldTag] = newFuncDef
	newFuncDef.Body = r.resolveExpr(funcDef.Body)
	return newTag
}

func (r *resolver) lookupVariant(oldTag string, params []string) string {
	newTag := r.nextTag()
	funcDef := r.in[oldTag]
	r.variants = append(r.variants, &sem.FuncDef{
		AST:    funcDef.AST,
		Tag:    newTag,
		Name:   funcDef.Name,
		Params: params,
		Body:   r.resolveExpr(funcDef.Body), // since params have been bound this will convert the CallParams
	})
	return newTag
}

func (r *resolver) nextTag() string {
	tag := strconv.Itoa(r.ntag)
	r.ntag++
	return tag
}

func (r *resolver) pushParams(scope map[string]string) {
	r.params = append(r.params, scope)
}

func (r *resolver) popParams() {
	r.params = r.params[0 : len(r.params)-1]
}

func (r *resolver) lookupParam(param string) string {
	if len(r.params) == 0 {
		return ""
	}
	return r.params[len(r.params)-1][param]
}

func isBuiltin(tag string) bool {
	_, err := strconv.Atoi(tag)
	return err != nil
}
