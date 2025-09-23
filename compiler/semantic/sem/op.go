// Package sem represents an semantic-internal AST representation that
// is used for analysis between the AST and DAG representations.
// This is never serialized (though it is output by sfmt) so there are
// no Kind fields or unpacker hooks.  The sem AST has no scope and all
// functions, ops, etc are resolved to flattened tables.  Likewise, all SQL
// structure is transalted to native dataflow here with backpointers to the
// original ast nodes for error reporting.
// XXX Calls are resolved to what they are...
//   values call
//   aggregate agg etc
// XXX globs translated to regexps (with backpointer to glob expr)
// from entities are flattened (e.g., globs on file system generate multiple file scans)
// Text entities resolved to strings etc
// TupleExprs translated to record expressions

// XXX can also be json serialized but that's just for devs and we can leave
// in a camel case form w/o json tags etc.  Also json serialization will be really
// noisy because of the embedded AST references

package sem

import (
	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/order"
	"github.com/segmentio/ksuid"
)

type Main struct {
	Funcs []*FuncDef
	Body  Seq
}

// Op is the interface implemented by all AST operator nodes.
type Op interface {
	opNode()
}

// Scanner ops implement both Scanner and Op
type (
	CommitMetaScan struct {
		AST    ast.FromEntity
		Pool   ksuid.KSUID
		Commit ksuid.KSUID
		Meta   string
		Tap    bool
	}
	DBMetaScan struct {
		AST  *ast.DBMeta
		Meta string
	}
	DefaultScan struct{}
	DeleteScan  struct {
		AST    *ast.Delete
		ID     ksuid.KSUID
		Commit ksuid.KSUID
		Where  Expr
	}
	FileScan struct {
		AST    *ast.FromElem
		Path   string
		Format string
	}
	HTTPScan struct {
		AST     *ast.FromElem
		URL     string
		Format  string
		Method  string
		Headers map[string][]string
		Body    string
	}
	NullScan     struct{}
	PoolMetaScan struct {
		AST  ast.FromEntity
		ID   ksuid.KSUID
		Meta string
	}
	PoolScan struct {
		AST    ast.FromEntity
		ID     ksuid.KSUID
		Commit ksuid.KSUID
	}
	RobotScan struct {
		AST    *ast.ExprEntity
		Expr   Expr
		Format string
	}
)

func (*CommitMetaScan) opNode() {}
func (*DBMetaScan) opNode()     {}
func (*DefaultScan) opNode()    {}
func (*DeleteScan) opNode()     {}
func (*FileScan) opNode()       {}
func (*HTTPScan) opNode()       {}
func (*NullScan) opNode()       {}
func (*PoolMetaScan) opNode()   {}
func (*PoolScan) opNode()       {}
func (*RobotScan) opNode()      {}

type TableAlias struct { // this can probably go away
	AST *ast.TableAlias
}

type FuncDef struct {
	AST    ast.Expr // body of function or lambda value
	Tag    string
	Name   string
	Params []string
	Body   Expr
}

// XXX expand these but leave AST node for error reporting?,
// if so, we can get rid of this and just have refs (OpResolved?).
// then maybe get rid of them?
type OpDecl struct {
	AST  *ast.OpDecl // Name, Params
	Body Seq
}

// ----------------------------------------------------------------------------
// Operators

// A Seq represents a sequence of operators that receive
// a stream of values from their parent into the first operator
// and each subsequent operator processes the output records from the
// previous operator.
type Seq []Op

func (s *Seq) Prepend(front Op) { //XXX do we need this?
	*s = append([]Op{front}, *s...)
}

// An Op is a node in the flowgraph that takes values in, operates upon them,
// and produces values as output.
type (
	AggregateOp struct {
		AST   ast.Op // ast.Aggregate or ast.OpExpr
		Limit int
		Keys  []Assignment
		Aggs  []Assignment
	}
	AssertOp struct {
		AST  *ast.Assert
		Kind string
		Expr Expr
		Text string
	}
	BadOp struct{}
	//XXX might want to expand these in first pass to CallOpRefs (with expanded ast inside)...
	CallOp struct {
		AST  *ast.CallOp // Name here
		Args []Expr      `json:"args"`
	}
	CutOp struct {
		AST  *ast.Cut
		Args []Assignment
	}
	DebugOp struct {
		AST  *ast.Debug
		Expr Expr
	}
	DistinctOp struct {
		AST  *ast.Distinct
		Expr Expr
	}
	DropOp struct {
		AST  *ast.Drop
		Args []Expr
	}
	ExplodeOp struct {
		AST  *ast.Explode
		Args []Expr
		Type string
		As   string
	}
	FilterOp struct {
		AST  ast.Op // ast.Where, ast.OpExpr, ast.Search
		Expr Expr
	}
	ForkOp struct {
		Paths []Seq
	}
	FuseOp struct {
		AST *ast.Fuse
	}
	HeadOp struct {
		AST   *ast.Head
		Count int
	}
	JoinOp struct {
		AST        ast.Op // CrossJoin, SQL*Join, Join, etc (might not need this)
		Kind       string
		Style      string
		LeftAlias  string
		RightAlias string
		Cond       Expr
	}
	LoadOp struct {
		AST     *ast.Load
		Pool    ksuid.KSUID
		Branch  string
		Author  string
		Message string
		Meta    string
	}
	MergeOp struct {
		AST   *ast.Merge
		Exprs []SortExpr
	}
	OutputOp struct {
		AST  ast.Op
		Name string
	}
	PutOp struct {
		AST  ast.Op
		Args []Assignment
	}
	RenameOp struct {
		AST  *ast.Rename
		Args []Assignment
	}
	SearchOp struct {
		AST  *ast.Search
		Expr Expr
	}
	ShapesOp struct {
		AST  *ast.Shapes
		Expr Expr
	}
	SkipOp struct {
		AST   *ast.Skip
		Count Expr
	}
	SortOp struct {
		AST     *ast.Sort
		Exprs   []SortExpr
		Reverse bool
	}
	SwitchOp struct {
		AST   *ast.Switch
		Expr  Expr
		Cases []Case
	}
	TailOp struct {
		AST   *ast.Tail
		Count int
	}
	TopOp struct {
		AST     *ast.Top
		Limit   int
		Exprs   []SortExpr
		Reverse bool
	}
	UniqOp struct {
		AST   *ast.Uniq
		Cflag bool
	}
	UnnestOp struct {
		AST  *ast.Unnest
		Expr Expr
		Body Seq
	}
	ValuesOp struct {
		AST   ast.Op // ast.Values or ast.OpExpr
		Exprs []Expr
	}
)

// Suport structs for Ops
type (
	Assignment struct {
		AST *ast.Assignment
		LHS Expr
		RHS Expr
	}
	// Case paths inside of SwitchOp
	Case struct {
		Expr Expr
		Path Seq
	}
	SortExpr struct {
		AST   ast.SortExpr
		Expr  Expr
		Order order.Which
		Nulls order.Nulls
	}
)

func (*AggregateOp) opNode() {}
func (*AssertOp) opNode()    {}
func (*BadOp) opNode()       {}
func (*CallOp) opNode()      {}
func (*CutOp) opNode()       {}
func (*DebugOp) opNode()     {}
func (*DistinctOp) opNode()  {}
func (*DropOp) opNode()      {}
func (*ExplodeOp) opNode()   {}
func (*FilterOp) opNode()    {}
func (*ForkOp) opNode()      {}
func (*FuseOp) opNode()      {}
func (*HeadOp) opNode()      {}
func (*JoinOp) opNode()      {}
func (*LoadOp) opNode()      {}
func (*MergeOp) opNode()     {}
func (*OutputOp) opNode()    {}
func (*PutOp) opNode()       {}
func (*RenameOp) opNode()    {}
func (*SearchOp) opNode()    {}
func (*ShapesOp) opNode()    {}
func (*SkipOp) opNode()      {}
func (*SortOp) opNode()      {}
func (*SwitchOp) opNode()    {}
func (*TailOp) opNode()      {}
func (*TopOp) opNode()       {}
func (*UnnestOp) opNode()    {}
func (*UniqOp) opNode()      {}
func (*ValuesOp) opNode()    {}

type AggFunc struct {
	AST      ast.Expr
	Name     string // convert to lower case
	Distinct bool
	Expr     Expr
	Where    Expr
}

func NewValues(o ast.Op, expr ...Expr) *ValuesOp {
	return &ValuesOp{AST: o, Exprs: expr}
}
