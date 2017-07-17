package merge

import (
	"github.com/influxdata/influxdb/influxql"
	"strings"
	"fmt"
	"strconv"
)

type ResultSource interface {
	Next(string) []float64
}

type QueryField struct {
	Root       	  QueryNode
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

type QueryTree struct {
	Fields []QueryField
}

func NewQueryTree(stmt *influxql.SelectStatement) (*QueryTree, *QueryBuilder, error) {
	tree := &QueryTree{}
	tree.Fields = []QueryField{}
	qb := NewQueryBuilder()
	for _, field := range stmt.Fields {
		qField := QueryField{ResponseField:field.Name()}
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
		// Aggregations
		case "integral", "median", "stddev":
			return nil, fmt.Errorf("Not yet supported")
		// Selectors
		case "sample", "percentile", "first", "last":
			// First and Last can not be supported as
			// Remember that for "sample", the the values timestamps will be returned
			// which differs from the assumption that the timestamp is at the start of the group.
			return nil, fmt.Errorf("Not yet supported")

		default:
			return nil, fmt.Errorf("InfluxQL function %s is not supported when merging results from multiple hosts.", f.Name)
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
	default:
		return nil, fmt.Errorf("Unknown expression '%s' of type %T", f.String(), f)
	}
	return n, nil
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
	op influxql.Token
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
	}
	var res float64
	switch n.op {
	case influxql.ADD: res = lhs[0] + rhs[0]
	case influxql.SUB: res = lhs[0] - rhs[0]
	case influxql.MUL: res = lhs[0] * rhs[0]
	case influxql.DIV:
		if rhs[0] != 0 {
			res = lhs[0] / rhs[0]
		} else {
			res = 0
		}
	case influxql.MOD: res = float64(int(lhs[0]) % int(rhs[0]))
	}
	return []float64{res}
}
