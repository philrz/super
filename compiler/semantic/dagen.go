package semantic

import (
	"github.com/brimdata/super"
	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/compiler/dag"
	"github.com/brimdata/super/compiler/semantic/sem"
)

type dagen struct {
	//outputs map[*dag.Output]ast.Node // why point to Node?
	outputs map[*dag.Output]any
}

func (d *dagen) compile(seq *sem.Main) *dag.Main {

}

func evalAtCompileTime(sctx *super.Context, e sem.Expr) (super.Value, error) {
	panic("TBD")
}

// XXX
func (d *dagen) debugOp(o *sem.DebugOp, mainAst ast.Seq, in sem.Seq) sem.Seq {
	output := &dag.Output{Kind: "Output", Name: "debug"}
	d.outputs[output] = o
	e := d.expr(o.Expr)
	y := &dag.Values{Kind: "Values", Exprs: []dag.Expr{e}}
	main := d.seq(mainAst)
	if len(main) == 0 {
		main.Append(&sem.Pass{Kind: "Pass"})
	}
	return append(in, &dag.Mirror{
		Kind:   "Mirror",
		Main:   main,
		Mirror: sem.Seq{y, output},
	})
}

func (d *dagen) exprNullable(e sem.Expr) dag.Expr {
	panic("TBD")
}
