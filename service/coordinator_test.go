package service

import (
	"fmt"
	"github.com/adamringhede/influxdb-ha/service/merge"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

type ResultSourceMock struct {
	data         []resultGroup
	fieldIndices map[string]int
	i            int
}

type resultGroup struct {
	time   string
	values []interface{}
}

func NewResultSourceMock(results []result) *ResultSourceMock {
	source := &ResultSourceMock{
		data:         []resultGroup{},
		fieldIndices: map[string]int{},
	}

	// Group values by time and ensure order. This assumes that both results have the exact same time-steps and
	// that times are strings.
	a := map[string]int{}
	ai := 0
	for _, res := range results {
		for _, series := range res.Series {
			for _, v := range series.Values {
				k := string(v[0].(string))
				if _, ok := a[k]; !ok {
					source.data = append(source.data, resultGroup{k, []interface{}{}})
					a[k] = ai
					ai += 1
				}
				group := source.data[a[k]]
				source.data[a[k]].values = append(group.values, v)
			}
		}
	}

	for i, col := range results[0].Series[0].Columns {
		source.fieldIndices[col] = i
	}

	return source
}

func (s *ResultSourceMock) Next(fieldKey string) []float64 {
	res := make([]float64, len(s.data[s.i].values))
	fieldIndex, fieldExists := s.fieldIndices[fieldKey]
	if !fieldExists {
		panic(fmt.Errorf("No values exist for field key %s", fieldKey))
	}
	if s.Done() {
		return res
	}
	for i, v := range s.data[s.i].values {
		if data, ok := v.([]interface{}); ok {
			switch value := data[fieldIndex].(type) {
			case int: res[i] = float64(value)
			case float64: res[i] = value
			default:
				panic(fmt.Errorf("Unsupported type %T", value))
			}

		}
	}
	return res
}

func (s *ResultSourceMock) Step() {
	s.i += 1
}

func (s *ResultSourceMock) Done() bool {
	return s.i >= len(s.data)
}

func (s *ResultSourceMock) Reset() {
	s.i = 0
}

func Test_buildTree(t *testing.T) {
	stmt := mustGetSelect(`SELECT top("foo", 2), mean("value") FROM sales where time < now() group by time(1h) `)
	_, _, err := merge.NewQueryTree(stmt)
	assert.NoError(t, err)
}

func Test_mergeResults(t *testing.T) {

	a := result{Series: []*models.Row{
		{
			Columns: []string{"time", "sum_value_", "top_value__1_", "mean_value_", "count_value_"},
			Values: [][]interface{}{
				{"1970-01-01T00:00:00Z", 5.0, 5.0, 5.0, 1},
				{"1970-01-02T00:00:00Z", 8.0, 8.0, 8.0, 1},
			}},
	}}

	b := result{Series: []*models.Row{
		{
			Columns: []string{"time", "sum_value_", "top_value__1_", "mean_value_", "count_value_"},
			Values: [][]interface{}{
				{"1970-01-01T00:00:00Z", 3.0, 3.0, 3.0, 1},
				{"1970-01-02T00:00:00Z", 12.0, 12.0, 12.0, 1},
			}},
	}}

	// The results have toe be grouped by tags
	source := NewResultSourceMock([]result{a, b})

	assert.Equal(t, []float64{5, 3}, source.Next("sum_value_"))
	source.Step()
	assert.Equal(t, []float64{8, 12}, source.Next("sum_value_"))
	source.Reset()

	stmt := mustGetSelect(`SELECT mean("value"), top("value", 1), sum("value"), mean("value") * 3 FROM sales where time < now() group by time(1d) `)
	tree, _, err := merge.NewQueryTree(stmt)
	assert.NoError(t, err)
	assert.Equal(t, []float64{4}, tree.Fields[0].Root.Next(source))
	assert.Equal(t, []float64{5}, tree.Fields[1].Root.Next(source))
	assert.Equal(t, []float64{8}, tree.Fields[2].Root.Next(source))
	assert.Equal(t, []float64{12}, tree.Fields[3].Root.Next(source))
	source.Step()
	assert.Equal(t, []float64{10}, tree.Fields[0].Root.Next(source))
	assert.Equal(t, []float64{12}, tree.Fields[1].Root.Next(source))
	assert.Equal(t, []float64{20}, tree.Fields[2].Root.Next(source))
	assert.Equal(t, []float64{30}, tree.Fields[3].Root.Next(source))
	source.Reset()
}

func mustGetSelect(q string) *influxql.SelectStatement {
	query, err := influxql.ParseQuery(q)
	if err != nil {
		panic(err)
	}
	stmts := getSelectStatements(query.Statements)
	if len(stmts) != 1 {
		panic(fmt.Errorf("Expected to receive 1 statement from query. Got %d", len(stmts)))
	}
	return stmts[0]
}

func getSelectStatements(statements influxql.Statements) (result []*influxql.SelectStatement) {
	for _, stmt := range statements {
		switch s := stmt.(type) {
		case *influxql.SelectStatement:
			result = append(result, s)
		}
	}
	return result
}
