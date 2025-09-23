package sem

import (
	"github.com/brimdata/super/compiler/ast"
)

type Expr interface {
	exprNode()
}

//XXX get rid of VectorValue is separate PR... can just use assertion on Expr

// XXX alphabetize

// The type definitions of all entities that implement the Expr interface.
type (
	ArrayExpr struct {
		AST   *ast.ArrayExpr
		Elems []Expr
	}
	BadExpr     struct{}
	BetweenExpr struct {
		AST   *ast.Between
		Expr  Expr
		Lower Expr
		Upper Expr
	}
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
	CallExtract struct {
		AST  *ast.CallExtract
		Part Expr
		Expr Expr
	}
	CaseExpr struct {
		AST   *ast.CaseExpr
		Expr  Expr
		Whens []When
		Else  Expr
	}
	CastExpr struct {
		AST  *ast.Cast
		Expr Expr
		Type Expr
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
	// XXX what is this?
	Exists struct {
		AST  *ast.Exists
		Body Seq
	}
	FieldExpr struct {
		AST   ast.Expr // Could be an inferred expr or ast.FieldExpr
		Name  string
		Value Expr
	}
	// XXX we can probably get rid of this but keep for now
	FStringExpr struct {
		AST  *ast.FStringExpr
		Expr Expr
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
	LambdaExpr struct {
		AST  *ast.Lambda // Params here
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
	// Record elements has been resolved to name:expr or it's a spread
	RecordExpr struct {
		AST   ast.Expr // ast.TupleExpr or ast.RecordExpr
		Elems []Expr
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
		Elems []Expr
	}
	SliceExpr struct {
		AST  ast.Expr
		Expr Expr
		From Expr
		To   Expr
	}
	SpreadExpr struct {
		AST  *ast.Spread
		Expr Expr
	}
	StructuredError struct {
		AST     ast.Expr
		Message string
		On      Expr
	}
	SubqueryExpr struct {
		AST        ast.Expr
		Correlated bool
		Array      bool
		Body       Seq
	}
	// XXX backward compat until better time data types
	SQLTimeValue struct {
		AST   *ast.SQLTimeValue // Type here (must be string?!)
		Value string            // sup value
	}
	//Keep this for error reporting even though resolved? or error reporting is done
	// after it's resolved? XXX we can look here as to what needs it
	Text struct {
		AST *ast.Text
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

// When is used by CaseExpr
type When struct {
	AST  *ast.When
	Cond Expr
	Then Expr
}

// Sum types for array, set, and record bodies

type ()

func (*ArrayExpr) exprNode()        {}
func (*BadExpr) exprNode()          {}
func (*BinaryExpr) exprNode()       {}
func (*BetweenExpr) exprNode()      {}
func (*CondExpr) exprNode()         {}
func (*CallExpr) exprNode()         {}
func (*CallExtract) exprNode()      {}
func (*CaseExpr) exprNode()         {}
func (*CastExpr) exprNode()         {}
func (*DotExpr) exprNode()          {}
func (*Exists) exprNode()           {}
func (*FieldExpr) exprNode()        {}
func (*IndexExpr) exprNode()        {}
func (*IsNullExpr) exprNode()       {}
func (*LambdaExpr) exprNode()       {}
func (*LiteralExpr) exprNode()      {} //XXX call this PrimitiveExpr?
func (*MapCallExpr) exprNode()      {}
func (*MapExpr) exprNode()          {}
func (*RecordExpr) exprNode()       {}
func (*RegexpMatchExpr) exprNode()  {}
func (*RegexpSearchExpr) exprNode() {}
func (*SearchTermExpr) exprNode()   {} // XXX SearchTerm? => should be converted to normal expr
func (*SetExpr) exprNode()          {}
func (*SliceExpr) exprNode()        {}
func (*SpreadExpr) exprNode()       {}
func (*SQLTimeValue) exprNode()     {}
func (*StructuredError) exprNode()  {}
func (*SubqueryExpr) exprNode()     {}
func (*ThisExpr) exprNode()         {}
func (*UnaryExpr) exprNode()        {}

func (*Assignment) exprNode() {} //XXX seems like this shouldn't be expression (except lval for stuff)
func (*AggFunc) exprNode()    {} //XXX seems like this shouldn't be expression

// FuncRef is a pseudo-expression that represents a function reference as a value.
// It is not used by the runtime (but could be if we wanted to support this).  Instead,
// the semantic pass uses this in a first stage to represent lambda-parameterized functions
// then in a second stage it unrolls them all into regular calls by creating a unique
// new function for each combination of passed in lambdas.
type FuncRef struct {
	AST ast.Expr // can be lambda use or the function name reference
	Tag string
}

//XXX move this into sem

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
