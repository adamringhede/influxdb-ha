package router_test

import (
	"testing"
	"github.com/influxdata/influxdb/influxql"
	"log"
)

func getSelectStatements(statements influxql.Statements) (result []*influxql.SelectStatement) {
	for _, stmt := range statements {
		switch s := stmt.(type){
		case *influxql.SelectStatement: result = append(result, s)
		}
	}
	return result
}

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
			greater than and less than operators could also work here to limit
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

func TestFindShard(t *testing.T) {

	//queryParams := "SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"=1 AND (\"region\"!='us-west' AND \"host\"='server01'); SELECT * FROM test"
	queryParams := `SELET * FROM (SELECT "active" / "total" FROM "mem" WHERE "region"='us-west' AND time > now() - 2m GROUP BY *)`
	//queryParams := `SELECT BOTTOM("water_level",3), MEAN("water_level") / SUM("water_level") FROM "h2o_feet"`
	q, parseErr := influxql.ParseQuery(queryParams)
	ers := string(parseErr.Error())
	log.Print(ers)
	if parseErr != nil {
		log.Panic(parseErr)
	}

	for _, stmt := range getSelectStatements(q.Statements) {
		finder := &tagFinder{make(map[string]bool), make(map[string][]string)}
		for _, condition := range findConditions(stmt) {
			finder.findTags(condition)
		}
		log.Print(finder)
	}

	/*
	Create a map to hold all tag keys
	If the operation is not "=" then set the value to false
	If the operation is "=" and the current value is not false, then set it to true
	Store all tag values in "=" operations in a [string]map

	By doing this, we can effectively limit the shards we need to query.
	If the tag availability map does not contain all the tag keys needed for the shard key on
	the measurement, then the query has to be made to all shards in the cluster.

	Now pick out that tags where value is true and see if it satisfies the shard key.
	If it does, then lookup the chunk matching that key and then send the query
	to that chunk's shard (pre migration). If the chunk is being migrated, the query
	should be passed to the old chunk version. This means that for it to include new
	data, the writer has to write data to both shards during a migration. After
	the migration, the migrator will drop all the series matching its shard key range
	from the old shard. Maybe it can also wait with dropping the series until all queries
	on that measurement has finished.

	shardKey := "host"



	 */

}