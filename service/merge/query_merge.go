package merge

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/influxql"
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
		default:
			/*
				Not supported:
				integral, median, stddev, sample, percentile, first, last
				as well as all transformations

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
