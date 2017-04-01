package router

import "github.com/influxdata/influxdb/influxql"

type tagFinder struct {
	tags	map[string]bool
	values	map[string][]string // it could be interface to allow for numerical values as well
}

func (f *tagFinder) putValue(key string, value interface{}) {
	_, ok := f.values[key]
	if !ok {
		f.values[key] = []string{}
	}
	switch v := value.(type) {
	case *influxql.StringLiteral: f.values[key] = append(f.values[key], v.Val)
	}

}

func (f *tagFinder) findTags(cond influxql.Expr) {
	// if op isn't 22 or 23, then it is some equality operation.
	switch expr := cond.(type) {
	case *influxql.ParenExpr: f.findTags(expr.Expr)
	case *influxql.BinaryExpr:
		if expr.Op == influxql.AND || expr.Op == influxql.OR {
			f.findTags(expr.LHS)
			f.findTags(expr.RHS)
		} else {
			tagKey := expr.LHS.(*influxql.VarRef).Val

			/*
			TODO greater than and less than operators could also work here to limit
			the number of shards that need to be queried.
			 */
			if expr.Op == influxql.EQ {
				current, ok := f.tags[tagKey]
				if !ok || current != false {
					f.tags[tagKey] = true
					f.putValue(tagKey, expr.RHS)
				}
			} else {
				f.tags[tagKey] = false
			}
		}
	}
}

func findConditions(stmt *influxql.SelectStatement) []influxql.Expr {
	conditions := []influxql.Expr{}
	conditions = append(conditions, stmt.Condition)
	for _, source := range stmt.Sources {
		switch s := source.(type) {
		case *influxql.SubQuery:
			conditions = append(conditions, findConditions(s.Statement)...)
		}
	}
	return conditions
}

func findMeasurements(srcs []influxql.Source) []*influxql.Measurement {
	measurements := []*influxql.Measurement{}
	for _, source := range srcs {
		switch s := source.(type) {
		case *influxql.SubQuery:
			measurements = append(measurements, findMeasurements(s.Statement.Sources)...)
		case *influxql.Measurement:
			measurements = append(measurements, s)
		}
	}
	return measurements
}

type ShardFinder struct {
	//configSvr
}

func (f *ShardFinder) Find(stmt *influxql.SelectStatement) []string {
	finder := &tagFinder{make(map[string]bool), make(map[string][]string)}
	findMeasurements(stmt.Sources)
	for _, condition := range findConditions(stmt) {
		finder.findTags(condition)
	}
	// if a measurement is included without conditions in an ancestor statement, then all shards or the primary has to be included.
	// only if there are options and those fit the shard key should it be limited to fewer shards.

	// TODO get shard keys for selected measurements
	// If one of the selected measurements is not sharded, the primary RS has to be included.
	// TODO check if where clause is limiting enough for the shard keys. if not, return all shards.
	// TODO find chunks for selected measurements,
	// TODO get the set of nodes needed for found chunks matching condition values
	return []string{"rs1"}
}