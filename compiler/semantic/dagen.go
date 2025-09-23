package semantic

import (
	"errors"
	"slices"
	"strings"

	"github.com/brimdata/super"
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
	for _, f := range d.funcs {
		dagFuncs = append(dagFuncs, f)
	}
	// Sort function entries so they are consistently ordered by integer tag strings.
	slices.SortFunc(dagFuncs, func(a, b *dag.FuncDef) int {
		return strings.Compare(a.Tag, b.Tag)
	})
	return &dag.Main{Funcs: dagFuncs, Body: dagSeq}
}

func (d *dagen) seq(seq sem.Seq) dag.Seq {
	panic("TBD")
}

func (d *dagen) expr(seq sem.Expr) dag.Expr {
	panic("TBD")
}

// XXX
func (d *dagen) debugOp(o *sem.DebugOp, mainSem sem.Seq, in dag.Seq) dag.Seq {
	output := &dag.Output{Kind: "Output", Name: "debug"}
	d.outputs[output] = o
	e := d.expr(o.Expr)
	y := &dag.Values{Kind: "Values", Exprs: []dag.Expr{e}}
	main := d.seq(mainSem)
	if len(main) == 0 {
		//XXX do we need pass?
		main.Append(&dag.Pass{Kind: "Pass"})
	}
	return append(in, &dag.Mirror{
		Kind:   "Mirror",
		Main:   main,
		Mirror: dag.Seq{y, output},
	})
}

func (d *dagen) checkOutputs(isLeaf bool, seq dag.Seq) dag.Seq {
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

func (d *dagen) exprNullable(e sem.Expr) dag.Expr {
	panic("TBD")
}

func evalAtCompileTime(sctx *super.Context, e sem.Expr) (super.Value, error) {
	panic("TBD")
}
