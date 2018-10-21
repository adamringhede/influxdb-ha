package merge

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/influxdata/influxql"
)

type ResultSource interface {
	Next(string) []float64
}

type QueryField struct {
	Root          QueryNode
	ResponseField string
}

type QueryNode interface {
	Next(ResultSource) []float64
}

type QueryBuilder struct {
	Fields map[string]string
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{map[string]string{}}
}

func (qb *QueryBuilder) Get(expr string) *Values {
	name := expr
	for _, t := range []string{"\"", "'", "(", ")", ",", ".", " ", "\n"} {
		name = strings.Replace(name, t, "_", -1)
	}
	if _, ok := qb.Fields[expr]; !ok {
		qb.Fields[expr] = name
	}
	return &Values{qb.Fields[expr]}
}

// CreateStatement ...
// Implementation is taken from influxdb source.
func (qb *QueryBuilder) CreateStatement(s *influxql.SelectStatement) string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")
	for field, name := range qb.Fields {
		_, _ = buf.WriteString(field + " AS " + name + ",")
	}
	// Remove comma
	buf.Truncate(buf.Len() - 1)

	if s.Target != nil {
		_, _ = buf.WriteString(" ")
		_, _ = buf.WriteString(s.Target.String())
	}
	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	switch s.Fill {
	case influxql.NoFill:
		_, _ = buf.WriteString(" fill(none)")
	case influxql.NumberFill:
		_, _ = buf.WriteString(fmt.Sprintf(" fill(%v)", s.FillValue))
	case influxql.LinearFill:
		_, _ = buf.WriteString(" fill(linear)")
	case influxql.PreviousFill:
		_, _ = buf.WriteString(" fill(previous)")
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	if s.SLimit > 0 {
		_, _ = fmt.Fprintf(&buf, " SLIMIT %d", s.SLimit)
	}
	if s.SOffset > 0 {
		_, _ = fmt.Fprintf(&buf, " SOFFSET %d", s.SOffset)
	}
	return buf.String()
}

type QueryTree struct {
	Fields []QueryField
}

func NewQueryTree(stmt *influxql.SelectStatement) (*QueryTree, *QueryBuilder, error) {
	tree := &QueryTree{}
	tree.Fields = []QueryField{}
	qb := NewQueryBuilder()
	for _, field := range stmt.Fields {
		if _, isVarRef := field.Expr.(*influxql.VarRef); isVarRef {
			continue
		}
		qField := QueryField{ResponseField: field.Name()}
		node, err := createQueryNode(field.Expr, qb)
		if err != nil {
			return nil, nil, err
		}
		qField.Root = node
		tree.Fields = append(tree.Fields, qField)
	}
	return tree, qb, nil
}

func createQueryNode(expr influxql.Expr, qb *QueryBuilder) (QueryNode, error) {
	var n QueryNode
	switch f := expr.(type) {
	// TODO add support for wildcards and regular expressions
	case *influxql.StringLiteral:
		n = qb.Get(expr.String())
	case *influxql.Call:
		switch f.Name {
		case "mean":
			n = NewMean(f.Args[0].String(), qb)
		case "sum":
			n = NewSum(f.Args[0].String(), qb)
		case "top":
			count, _ := strconv.Atoi(f.Args[1].String())
			n = NewTop(f.Args[0].String(), count, qb)
		case "bottom":
			count, _ := strconv.Atoi(f.Args[1].String())
			n = NewBottom(f.Args[0].String(), count, qb)
		case "max":
			n = NewTop(f.Args[0].String(), 1, qb)
		case "min":
			n = NewBottom(f.Args[0].String(), 1, qb)
		case "spread":
			n = NewSpread(f.Args[0].String(), qb)
		case "distinct":
			n = NewDistinct(f.Args[0].String(), qb)
		case "mode":
			n = NewMode(f.Args[0].String(), qb)
		case "count":
			n = NewCount(f.Args[0].String(), qb)
		case "moving_average":
			width, _ := strconv.Atoi(f.Args[1].String())
			n = NewMovingAverage(f.Args[0].String(), width, qb)
		case "percentile":
			p, _ := strconv.ParseFloat(f.Args[1].String(), 64)
			n = NewPercentile(f.Args[0].String(), p, qb)
		case "median":
			n = NewMedian(f.Args[0].String(), qb)
		case "stddev":
			n = NewStddev(f.Args[0].String(), qb)
		case "abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "log2", "log10", "round", "sin", "sqrt", "tan":
			qn, err := createQueryNode(f.Args[0], qb)
			if err != nil {
				return nil, err
			}
			n = NewUnaryOp(qn, getUnaryOp(f.Name))
		default:
			/*
				Not supported:
				integral, sample, first, last,
				as well as all some transformations.

				First and Last can not be supported as ResultSource.Next only return values and
				not the timestamps of those value. Need a different function than next that also
				includes the time.
				Remember that for "sample", the the values timestamps will be returned
				which ´differs from the assumption that the timestamp is at the start of the group.
			*/
			return nil, fmt.Errorf("InfluxQL function '%s' is not supported when merging results from multiple hosts.", f.Name)
		}
	case *influxql.BinaryExpr:
		lhs, err := createQueryNode(f.LHS, qb)
		if err != nil {
			return nil, err
		}
		rhs, err := createQueryNode(f.RHS, qb)
		if err != nil {
			return nil, err
		}
		n = NewBinaryOp(f.Op, lhs, rhs)
	case *influxql.IntegerLiteral:
		n = NewFloatLit(float64(f.Val))
	case *influxql.NumberLiteral:
		n = NewFloatLit(f.Val)
	case *influxql.ParenExpr:
		expr, err := createQueryNode(f.Expr, qb)
		if err != nil {
			return nil, err
		}
		n = expr
	default:
		return nil, fmt.Errorf("Unknown expression '%s' of type %T", f.String(), f)
	}
	return n, nil
}

func getUnaryOp(name string) func (float64) float64 {
	switch name {
	case "abs":
		return math.Abs
	case "acos":
		return math.Acos
	case "asin":
		return math.Asin
	case "atan":
		return math.Atan
	//case "atan2":
	//	return math.Atan2
	case "ceil":
		return math.Ceil
	case "cos":
		return math.Cos
	//case "cumulative_sum":
	//	return math.cumulative_sum
	//case "derivative":
	//	return math.derivative
	//case "difference":
	//	return math.difference
	//case "elapsed":
	//	return math.elapsed
	//case "exp":
	//	return math.exp
	case "floor":
		return math.Floor
	//case "histogram":
	//	return math.histogram
	//case "ln":
	//	return math.Ln10
	case "log":
		return math.Log
	case "log2":
		return math.Log2
	case "log10":
		return math.Log10
	//case "non_negative_derivative":
	//	return math.non_negative_derivative
	//case "non_negative_difference":
	//	return math.non_negative_difference
	//case "pow":
	//	return math.pow
	case "round":
		return math.Round
	case "sin":
		return math.Sin
	case "sqrt":
		return math.Sqrt
	case "tan":
		return math.Tan
	}
	return func (value float64) float64 {
		return value
	}
}

type UnaryOp struct {
	values QueryNode
	fn func (float64) float64
}

func NewUnaryOp(qn QueryNode, fn func (float64) float64) *UnaryOp {
	return &UnaryOp{qn, fn}
}

func (n *UnaryOp) Next(source ResultSource) []float64 {
	values := n.values.Next(source)
	for i, v := range values {
		values[i] = n.fn(v)
	}
	return values
}

type FloatLit struct {
	val float64
}

func NewFloatLit(val float64) *FloatLit {
	return &FloatLit{val}
}

func (n *FloatLit) Next(source ResultSource) []float64 {
	return []float64{n.val}
}

type BinaryOp struct {
	op  influxql.Token
	lhs QueryNode
	rhs QueryNode
}

func NewBinaryOp(op influxql.Token, lhs, rhs QueryNode) *BinaryOp {
	return &BinaryOp{op, lhs, rhs}
}

func (n *BinaryOp) Next(source ResultSource) []float64 {
	lhs := n.lhs.Next(source)
	rhs := n.rhs.Next(source)
	if len(lhs) != 1 || len(rhs) != 1 {
		// This should not be possible in practice as only top and bottom may return multiple values
		// and they are not allowed in arithmetic operations.
		panic("Can not use arithmetic operations when there are multiple values in LHS and RHS")
		// FIXME Use proper error handling instead of panics so that we can return something proper to the client.
	}
	var res float64
	switch n.op {
	case influxql.ADD:
		res = lhs[0] + rhs[0]
	case influxql.SUB:
		res = lhs[0] - rhs[0]
	case influxql.MUL:
		res = lhs[0] * rhs[0]
	case influxql.DIV:
		if rhs[0] != 0 {
			res = lhs[0] / rhs[0]
		} else {
			res = 0
		}
	case influxql.MOD:
		res = float64(int(lhs[0]) % int(rhs[0]))
	}
	return []float64{res}
}
