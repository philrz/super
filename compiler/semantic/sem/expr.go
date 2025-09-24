package sem

import (
	"github.com/brimdata/super/compiler/ast"
	"github.com/brimdata/super/sup"
)

type Expr interface {
	exprNode()
}

//XXX get rid of VectorValue is separate PR... can just use assertion on Expr

// The type definitions of all entities that implement the Expr interface.
type (
	ArrayExpr struct {
		AST   *ast.ArrayExpr
		Elems []ArrayElem
	}
	BadExpr    struct{}
	BinaryExpr struct {
		AST ast.Expr
		Op  string // normalized Op string
		LHS Expr
		RHS Expr
	}
	// An ast.Call that has been resolved to an actual function call in expression
	// context... this now refers to the flattened function table (or a built-in) via tag
	CallExpr struct {
		AST  ast.Expr
		Tag  string
		Args []Expr
	}
	CondExpr struct {
		AST  ast.Expr
		Cond Expr
		Then Expr
		Else Expr
	}
	// A place to hold constants until we can eval them and check that they are constant.
	ConstExpr struct {
		AST  ast.Expr // pointer to const or type expression in AST
		Name *ast.ID  // pointer to ID in const or type decl
		Expr Expr     // this can be a type value too created from type foo=... compiled to <foo=...>
	}
	DotExpr struct {
		AST *ast.BinaryExpr
		LHS Expr
		RHS string
	}
	IndexExpr struct {
		AST   *ast.IndexExpr
		Expr  Expr
		Index Expr
	}
	IsNullExpr struct {
		AST  *ast.IsNullExpr // Not flag in here
		Expr Expr
	}
	LiteralExpr struct {
		AST   ast.Expr
		Value string
	}
	MapCallExpr struct {
		AST    *ast.Call
		Expr   Expr
		Lambda *CallExpr
	}
	MapExpr struct {
		AST     *ast.MapExpr
		Entries []Entry
	}
	RecordExpr struct {
		AST   ast.Expr // ast.TupleExpr or ast.RecordExpr
		Elems []RecordElem
	}
	RegexpMatchExpr struct {
		AST     ast.Expr // ast.Glob or ast.Regexp
		Pattern string
		Expr    Expr
	}
	RegexpSearchExpr struct {
		AST     ast.Expr // ast.Glob or ast.Regexp
		Pattern string
		Expr    Expr
	}
	SearchTermExpr struct {
		AST   ast.Expr
		Text  string
		Value string
		Expr  Expr
	}
	SetExpr struct {
		AST   *ast.SetExpr
		Elems []ArrayElem
	}
	SliceExpr struct {
		AST  ast.Expr
		Expr Expr
		From Expr
		To   Expr
	}
	SubqueryExpr struct {
		AST        ast.Expr
		Correlated bool
		Array      bool
		Body       Seq
	}
	ThisExpr struct {
		AST  ast.Expr // ast.ID, ast.BinaryExpr (dot), etc, SQL col/table before schema-path resolution
		Path []string
	}
	UnaryExpr struct {
		AST     ast.Expr
		Op      string // normalized Op (i.e., "not like" => "!")
		Operand Expr
	}
)

// Support structures embedded in Expr nodes

// Entry used by MapExpr
type Entry struct {
	Key   Expr
	Value Expr
}

// The sum type for array, set, and record elements.  There is not an easy way
// of creating a Go pseudo-type

type ArrayElem interface {
	arrayElemNode()
}

type RecordElem interface {
	recordElemNode()
}

type (
	FieldElem struct {
		AST   ast.Expr // Could be an inferred expr or ast.FieldExpr
		Name  string
		Value Expr
	}
	SpreadElem struct {
		AST  *ast.Spread
		Expr Expr
	}
	ExprElem struct {
		AST  *ast.Spread
		Expr Expr
	}
)

func (*ExprElem) arrayElemNode()    {}
func (*FieldElem) recordElemNode()  {}
func (*SpreadElem) arrayElemNode()  {}
func (*SpreadElem) recordElemNode() {}

func (*ArrayExpr) exprNode()        {}
func (*BadExpr) exprNode()          {}
func (*BinaryExpr) exprNode()       {}
func (*CondExpr) exprNode()         {}
func (*CallExpr) exprNode()         {}
func (*DotExpr) exprNode()          {}
func (*IndexExpr) exprNode()        {}
func (*IsNullExpr) exprNode()       {}
func (*LiteralExpr) exprNode()      {}
func (*MapCallExpr) exprNode()      {}
func (*MapExpr) exprNode()          {}
func (*RecordExpr) exprNode()       {}
func (*RegexpMatchExpr) exprNode()  {}
func (*RegexpSearchExpr) exprNode() {}
func (*SearchTermExpr) exprNode()   {}
func (*SetExpr) exprNode()          {}
func (*SliceExpr) exprNode()        {}
func (*SubqueryExpr) exprNode()     {}
func (*ThisExpr) exprNode()         {}
func (*UnaryExpr) exprNode()        {}

// FuncRef is a pseudo-expression that represents a function reference as a value.
// It is not used by the runtime (but could be if we wanted to support this).  Instead,
// the semantic pass uses this in a first stage to represent lambda-parameterized functions
// then in a second stage it unrolls them all into regular calls by creating a unique
// new function for each combination of passed in lambdas.
type FuncRef struct {
	AST ast.Expr // can be lambda use or the function name reference
	Tag string
}

// CallParam is a pseudo-expression that is like a call but represents the call
// of a FuncRef passed as an argument with the parameter name given by Param.
// It is not used by the runtime (but could be if we wanted to support this).  Instead,
// the semantic pass uses this in a first stage to represent abstract calls to functions
// passed as parameters, then in a second stage it flattens them all into regular calls
// by creating a unique new function for each combination of passed-in lambdas.
type CallParam struct {
	AST   *ast.Call
	Param string
	Args  []Expr
}

func (*FuncRef) exprNode()   {}
func (*CallParam) exprNode() {}

func NewThis(e ast.Expr, path []string) *ThisExpr {
	return &ThisExpr{AST: e, Path: path} //XXX AST? should have to include dummy message?
}

func NewBinaryExpr(e ast.Expr, op string, lhs, rhs Expr) *BinaryExpr {
	return &BinaryExpr{
		AST: e,
		Op:  op,
		LHS: lhs,
		RHS: rhs,
	}
}

func NewUnaryExpr(e ast.Expr, op string, operand Expr) *UnaryExpr {
	return &UnaryExpr{
		AST:     e,
		Op:      op,
		Operand: operand,
	}
}

func NewCall(e ast.Expr, tag string, args []Expr) *CallExpr {
	return &CallExpr{
		AST:  e,
		Tag:  tag,
		Args: args,
	}
}

// XXX change AST stuff to some sort of error/code ref interface
// XXX is this used?

func NewStructuredError(ref ast.Expr, message string, on Expr) Expr {
	rec := &RecordExpr{
		AST: ref,
		Elems: []RecordElem{
			&FieldElem{
				Name:  "message",
				Value: &LiteralExpr{AST: ref, Value: sup.String(message)},
			},
			&FieldElem{
				Name:  "on",
				Value: on,
			},
		},
	}
	return &CallExpr{
		AST:  ref,
		Tag:  "error",
		Args: []Expr{rec},
	}
}

// XXX
func (*AggFunc) exprNode() {}
