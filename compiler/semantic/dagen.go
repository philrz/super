package semantic

import (
	"errors"
	"slices"
	"strings"

	"github.com/brimdata/super/compiler/dag"
	"github.com/brimdata/super/compiler/semantic/sem"
)

type dagen struct {
	//outputs map[*dag.Output]ast.Node // why point to Node?
	outputs map[*dag.Output]any
	funcs   map[string]*dag.FuncDef
	t       *translator //XXX
}

func newDagen(t *translator) *dagen {
	return &dagen{
		outputs: make(map[*dag.Output]any), //XXX any? sem.Any?
		funcs:   make(map[string]*dag.FuncDef),
		t:       t,
	}
}

func (d *dagen) assemble(seq sem.Seq, funcs []*sem.FuncDef) *dag.Main {
	dagSeq := d.seq(seq)
	dagSeq = d.checkOutputs(true, dagSeq)
	dagFuncs := make([]*dag.FuncDef, 0, len(d.funcs))
	for _, f := range funcs {
		dagFuncs = append(dagFuncs, d.fn(f))
	}
	// Sort function entries so they are consistently ordered by integer tag strings.
	slices.SortFunc(dagFuncs, func(a, b *dag.FuncDef) int {
		return strings.Compare(a.Tag, b.Tag)
	})
	return &dag.Main{Funcs: dagFuncs, Body: dagSeq}
}

func (d *dagen) seq(seq sem.Seq) dag.Seq {
	var out dag.Seq
	for k, op := range seq {
		//XXX this doesn't seem right... why can't we just have
		// a runtime output entity that concurrently pulls from all
		// the debug channels (guaranteeing on deadlock) and pulls until
		// all are blocked and main DAG is done?
		if debugOp, ok := op.(*sem.DebugOp); ok {
			return d.debugOp(debugOp, seq[k+1:], out)
		}
		out = append(out, d.op(op))
	}
	return out
}

func (d *dagen) op(op sem.Op) dag.Op {
	switch op := op.(type) {
	case *sem.AggregateOp:
		return &dag.Aggregate{
			Kind:  "Aggregate",
			Limit: op.Limit,
			Keys:  d.assignments(op.Keys),
			Aggs:  d.assignments(op.Aggs),
		}
	case *sem.CutOp:
		return &dag.Cut{
			Kind: "Cut",
			Args: d.assignments(op.Args),
		}
	case *sem.DistinctOp:
		return &dag.Distinct{
			Kind: "Distinct",
			Expr: d.expr(op.Expr),
		}
	case *sem.DropOp:
		return &dag.Drop{
			Kind: "Drop",
			Args: d.exprs(op.Args),
		}
	case *sem.ExplodeOp:
		return &dag.Explode{
			Kind: "Explode",
			Args: d.exprs(op.Args),
			Type: op.Type,
			As:   op.As,
		}
	case *sem.FilterOp:
		return &dag.Filter{
			Kind: "Filter",
			Expr: d.expr(op.Expr),
		}
	case *sem.ForkOp:
		return &dag.Fork{
			Kind:  "Fork",
			Paths: d.paths(op.Paths),
		}
	case *sem.FuseOp:
		return &dag.Fuse{
			Kind: "Fuse",
		}
	case *sem.HeadOp:
		return &dag.Head{
			Kind:  "Head",
			Count: op.Count,
		}
	case *sem.JoinOp:
		return &dag.Join{
			Kind:       "Join",
			Style:      op.Style,
			LeftAlias:  op.LeftAlias,
			RightAlias: op.RightAlias,
			Cond:       d.expr(op.Cond),
		}
	case *sem.LoadOp:
		return &dag.Load{
			Kind:    "Load",
			Pool:    op.Pool,
			Branch:  op.Branch,
			Author:  op.Author,
			Message: op.Message,
			Meta:    op.Meta,
		}
	case *sem.MergeOp:
		return &dag.Merge{
			Kind:  "Merge",
			Exprs: d.sortExprs(op.Exprs),
		}
	case *sem.OutputOp:
		return &dag.Output{
			Kind: "Output",
			Name: op.Name,
		}
	case *sem.PutOp:
		return &dag.Put{
			Kind: "Put",
			Args: d.assignments(op.Args),
		}
	case *sem.RenameOp:
		return &dag.Rename{
			Kind: "Rename",
			Args: d.assignments(op.Args),
		}
	case *sem.SkipOp:
		return &dag.Skip{
			Kind:  "Skeip",
			Count: op.Count,
		}
	case *sem.SortOp:
		return &dag.Sort{
			Kind:    "Sort",
			Exprs:   d.sortExprs(op.Exprs),
			Reverse: op.Reverse,
		}
	case *sem.SwitchOp:
		return &dag.Switch{
			Kind:  "Switch",
			Expr:  d.expr(op.Expr),
			Cases: d.cases(op.Cases),
		}
	case *sem.TailOp:
		return &dag.Tail{
			Kind:  "Tail",
			Count: op.Count,
		}
	case *sem.TopOp:
		return &dag.Top{
			Kind:    "Top",
			Limit:   op.Limit,
			Exprs:   d.sortExprs(op.Exprs),
			Reverse: op.Reverse,
		}
	case *sem.UniqOp:
		return &dag.Uniq{
			Kind:  "Uniq",
			Cflag: op.Cflag,
		}
	case *sem.UnnestOp:
		return &dag.Unnest{
			Kind: "Unnest",
			Expr: d.expr(op.Expr),
			Body: d.seq(op.Body),
		}
	case *sem.ValuesOp:
		return &dag.Values{
			Kind:  "Values",
			Exprs: d.exprs(op.Exprs),
		}
	}
	panic(op)
}

func (d *dagen) paths(paths []sem.Seq) []dag.Seq {
	var out []dag.Seq
	for _, path := range paths {
		out = append(out, d.seq(path))
	}
	return out
}

func (d *dagen) cases(cases []sem.Case) []dag.Case {
	var out []dag.Case
	for _, c := range cases {
		out = append(out, dag.Case{Expr: d.expr(c.Expr), Path: d.seq(c.Path)})
	}
	return out
}

func (d *dagen) assignments(assignments []sem.Assignment) []dag.Assignment {
	var out []dag.Assignment
	for _, e := range assignments {
		out = append(out, dag.Assignment{
			Kind: "Assignment",
			LHS:  d.expr(e.LHS),
			RHS:  d.expr(e.RHS),
		})
	}
	return out
}

func (d *dagen) sortExprs(exprs []sem.SortExpr) []dag.SortExpr {
	var out []dag.SortExpr
	for _, e := range exprs {
		sortExpr := dag.SortExpr{
			Key:   d.expr(e.Expr),
			Order: e.Order,
			Nulls: e.Nulls,
		}
		out = append(out, sortExpr)
	}
	return out
}

func (d *dagen) exprs(exprs []sem.Expr) []dag.Expr {
	var out []dag.Expr
	for _, e := range exprs {
		out = append(out, d.expr(e))
	}
	return out
}

func (d *dagen) expr(e sem.Expr) dag.Expr {
	switch e := e.(type) {
	case *sem.ArrayExpr:
		return &dag.ArrayExpr{
			Kind:  "ArrayExpr",
			Elems: d.arrayElems(e.Elems),
		}
	case *sem.BinaryExpr:
		return &dag.BinaryExpr{
			Kind: "BinaryExpr",
			LHS:  d.expr(e.LHS),
			RHS:  d.expr(e.RHS),
		}
	case *sem.CallExpr:
		return d.call(e)
	case *sem.CondExpr:
		return &dag.Conditional{
			Kind: "Conditional",
			Cond: d.expr(e.Cond),
			Then: d.expr(e.Then),
			Else: d.expr(e.Else),
		}
	case *sem.DotExpr:
		return &dag.Dot{
			Kind: "Dot",
			LHS:  d.expr(e.LHS),
			RHS:  e.RHS,
		}
	case *sem.IndexExpr:
		return &dag.IndexExpr{
			Kind:  "IndexExpr",
			Expr:  d.expr(e.Expr),
			Index: d.expr(e.Index),
		}
	case *sem.IsNullExpr:
		return &dag.IsNullExpr{
			Kind: "IsNullExpr",
			Expr: d.expr(e.Expr),
		}
	case *sem.LiteralExpr:
		return &dag.Literal{ // XXX this should be called Primitive
			Kind:  "Literal",
			Value: e.Value,
		}
	case *sem.MapCallExpr:
		return &dag.MapCall{
			Kind:   "MapCall",
			Expr:   d.expr(e.Expr),
			Lambda: d.call(e.Lambda),
		}
	case *sem.MapExpr:
		return &dag.MapExpr{
			Kind:    "MapExpr",
			Entries: d.entries(e.Entries),
		}
	case *sem.RecordExpr:
		return &dag.RecordExpr{
			Kind:  "RecordExpr",
			Elems: d.recordElems(e.Elems),
		}
	case *sem.RegexpMatchExpr:
		return &dag.RegexpMatch{
			Kind:    "RegexpMatch",
			Pattern: e.Pattern,
			Expr:    d.expr(e.Expr),
		}
	case *sem.RegexpSearchExpr:
		return &dag.RegexpSearch{
			Kind:    "RegexpSearch",
			Pattern: e.Pattern,
			Expr:    d.expr(e.Expr),
		}
	case *sem.SearchTermExpr:
		return &dag.Search{
			Kind:  "Search",
			Text:  e.Text,
			Value: e.Value,
			Expr:  d.expr(e.Expr),
		}
	case *sem.SetExpr:
		return &dag.SetExpr{
			Kind:  "SetExpr",
			Elems: d.arrayElems(e.Elems),
		}
	case *sem.SliceExpr:
		return &dag.SliceExpr{
			Kind: "SliceExpr",
			Expr: d.expr(e.Expr),
			From: d.expr(e.From),
			To:   d.expr(e.To),
		}
	case *sem.SubqueryExpr:
		return d.subquery(e)
	case *sem.ThisExpr:
		return &dag.This{
			Kind: "This",
			Path: e.Path,
		}
	case *sem.UnaryExpr:
		return &dag.UnaryExpr{
			Kind:    "UnaryExpr",
			Op:      e.Op,
			Operand: d.expr(e.Operand),
		}
	}
	panic(e)
}

func (d *dagen) arrayElems(elems []sem.ArrayElem) []dag.VectorElem {
	var out []dag.VectorElem
	for _, elem := range elems {
		switch elem := elem.(type) {
		case *sem.SpreadElem:
			out = append(out, d.spread(elem.Expr))
		case *sem.ExprElem:
			out = append(out, &dag.VectorValue{Kind: "VectorValue", Expr: d.expr(elem.Expr)})
		default:
			panic(elem)
		}
	}
	return out
}

func (d *dagen) recordElems(elems []sem.RecordElem) []dag.RecordElem {
	var out []dag.RecordElem
	for _, elem := range elems {
		switch elem := elem.(type) {
		case *sem.SpreadElem:
			out = append(out, d.spread(elem.Expr))
		case *sem.FieldElem:
			out = append(out, &dag.Field{Kind: "Field", Name: elem.Name, Value: d.expr(elem.Value)})
		default:
			panic(elem)
		}
	}
	return out
}

func (d *dagen) spread(e sem.Expr) *dag.Spread {
	return &dag.Spread{
		Kind: "Spread",
		Expr: d.expr(e),
	}
}

func (d *dagen) entries(entries []sem.Entry) []dag.Entry {
	var out []dag.Entry
	for _, entry := range entries {
		out = append(out, dag.Entry{Key: d.expr(entry.Key), Value: d.expr(entry.Value)})
	}
	return out
}

func (d *dagen) subquery(e *sem.SubqueryExpr) *dag.Subquery {
	subquery := &dag.Subquery{
		Kind:       "Subquery",
		Correlated: e.Correlated,
		Body:       d.seq(e.Body),
	}
	if e.Array {
		subquery.Body = collectThis(subquery.Body)
	}
	return subquery
}

// XXX move this back to translator?
func collectThis(seq dag.Seq) dag.Seq {
	collect := dag.Assignment{
		Kind: "Assignment",
		LHS:  dag.NewThis([]string{"collect"}),
		RHS:  &dag.Agg{Kind: "Agg", Name: "collect", Expr: dag.NewThis(nil)},
	}
	aggOp := &dag.Aggregate{
		Kind: "Aggregate",
		Aggs: []dag.Assignment{collect},
	}
	emitOp := &dag.Values{
		Kind:  "Values",
		Exprs: []dag.Expr{dag.NewThis([]string{"collect"})},
	}
	seq = append(seq, aggOp)
	return append(seq, emitOp)
}

func (d *dagen) call(c *sem.CallExpr) *dag.Call {
	return &dag.Call{
		Kind: "Call",
		Tag:  c.Tag,
		Args: d.exprs(c.Args),
	}
}

func (d *dagen) fn(f *sem.FuncDef) *dag.FuncDef {
	return &dag.FuncDef{
		Kind:   "FuncDef",
		Tag:    f.Tag,
		Name:   f.Name,
		Params: f.Params,
		Expr:   d.expr(f.Body),
	}
}

func (d *dagen) debugOp(o *sem.DebugOp, branch sem.Seq, seq dag.Seq) dag.Seq {
	output := &dag.Output{Kind: "Output", Name: "debug"}
	d.outputs[output] = o
	e := d.expr(o.Expr)
	y := &dag.Values{Kind: "Values", Exprs: []dag.Expr{e}}
	mainBranch := d.seq(branch)
	if len(mainBranch) == 0 {
		//XXX do we need pass?
		mainBranch.Append(&dag.Pass{Kind: "Pass"})
	}
	return append(seq, &dag.Mirror{
		Kind:   "Mirror",
		Main:   mainBranch,
		Mirror: dag.Seq{y, output},
	})
}

// We should separate adding default outputs (do that in rungen) from checking
// for bad output ops.

// checkOutputs traverses the DAG and reports an error if any output
// nodes are in non-leave positions and as an output "main" to each
// leaf that is not connected
// - Report an error in any outputs are not located in the leaves.
// - Add output operators to any leaves where they do not exist.
func (d *dagen) checkOutputs(isLeaf bool, seq dag.Seq) dag.Seq {
	if len(seq) == 0 {
		return seq
	}

	lastN := len(seq) - 1
	for i, o := range seq {
		isLast := lastN == i
		switch o := o.(type) {
		case *dag.Output:
			if !isLast || !isLeaf {
				//XXX
				//n, ok := d.outputs[o]
				//if !ok {
				//	panic("system error: untracked user output")
				//}
				d.t.error(nil /*XXX*/, errors.New("output operator must be at flowgraph leaf"))
			}
		case *dag.Scatter:
			for k := range o.Paths {
				o.Paths[k] = d.checkOutputs(isLast && isLeaf, o.Paths[k])
			}
		case *dag.Unnest:
			o.Body = d.checkOutputs(false, o.Body)
		case *dag.Fork:
			for k := range o.Paths {
				o.Paths[k] = d.checkOutputs(isLast && isLeaf, o.Paths[k])
			}
		case *dag.Switch:
			for k := range o.Cases {
				o.Cases[k].Path = d.checkOutputs(isLast && isLeaf, o.Cases[k].Path)
			}
		case *dag.Mirror:
			o.Main = d.checkOutputs(isLast && isLeaf, o.Main)
			o.Mirror = d.checkOutputs(isLast && isLeaf, o.Mirror)
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
