package router

import (
	"github.com/influxdata/influxql"
	"log"
	"testing"
	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/coordinator"
)

func getSelectStatements(statements influxql.Statements) (result []*influxql.SelectStatement) {
	for _, stmt := range statements {
		switch s := stmt.(type) {
		case *influxql.SelectStatement:
			result = append(result, s)
		}
	}
	return result
}

func TestFindShard(t *testing.T) {

	//queryParams := "SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"=1 AND (\"region\"!='us-west' AND \"host\"='server01'); SELECT * FROM test"

	queryParams := `SELECT * FROM treasures WHERE type = 'gold'`
	//queryParams := `SELET * FROM (SELECT "active" / "total" FROM "mem" WHERE "region"='us-west' AND time > now() - 2m GROUP BY *)`
	//queryParams := `SELECT BOTTOM("water_level",3), MEAN("water_level") / SUM("water_level") FROM "h2o_feet"`
	q, parseErr := influxql.ParseQuery(queryParams)
	if parseErr != nil {
		log.Panic(parseErr)
	}

	for _, stmt := range getSelectStatements(q.Statements) {
		itrs, err := influxql.Select(stmt, coordinator.IteratorCreator{}, &influxql.SelectOptions{})
		spew.Dump(itrs)
		spew.Dump(err)
		break
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
