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

func (qb *QueryBuilder) Get(expr string) *Expr {
	name := expr
	for _, t := range []string{"\"", "'", "(", ")"} {
		name = strings.Replace(name, t, "_", -1)
	}
	if _, ok := qb.Fields[expr]; !ok {
		qb.Fields[expr] = name
	}
	return &Expr{qb.Fields[expr]}
}

type QueryTree struct {
	Fields []QueryField
}

func NewQueryTree(stmt *influxql.SelectStatement) (*QueryTree, *QueryBuilder, error) {
	tree := &QueryTree{}
	tree.Fields = []QueryField{}
	qb := NewQueryBuilder()
	for _, field := range stmt.Fields {
		qfield := QueryField{ResponseField:field.Name()}
		switch f := field.Expr.(type) {
		case *influxql.Call:
			switch f.Name {
			case "mean":
				qfield.Root = NewMean(f.Args[0].String(), qb)
			case "sum":
				qfield.Root = NewSum(f.Args[0].String(), qb)
			case "top":
				count, _ := strconv.Atoi(f.Args[1].String())
				qfield.Root = NewTop(f.Args[0].String(), count, qb)
			default:
				return nil, nil, fmt.Errorf("InfluxQL function %s is not supported when merging results from multiple hosts.", f.Name)
			}
		}
		tree.Fields = append(tree.Fields, qfield)
	}
	return tree, qb, nil
}