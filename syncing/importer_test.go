package syncing

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"testing"

	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
	)

var influxOne, _ = NewInfluxClientHTTP("127.0.0.1:28086", "", "")
var influxTwo, _ = NewInfluxClientHTTP("127.0.0.1:27086", "", "")
var influxThree, _ = NewInfluxClientHTTP("127.0.0.1:26086", "", "")
const etcdLoc = "127.0.0.1:2379"
const testDB = "sharded"

func multiple(location *InfluxClient, commands []string) {
	for _, q := range commands {
		_, err := location.Query(influx.NewQuery(q, "", "ns"))
		if err != nil {
			panic(err)
		}
	}
}

func initiate() {
	multiple(influxOne, []string{
		"DROP DATABASE " + testDB,
		"CREATE DATABASE " + testDB,
	})
	multiple(influxTwo, []string{
		"DROP DATABASE " + testDB,
	})
	time.Sleep(500 * time.Millisecond)
}

func Test_fetchLocationMeta(t *testing.T) {
	initiate()

	meta, err := fetchLocationMeta(influxOne)
	assert.Contains(t, meta.databases, testDB)
	assert.NoError(t, err)
}


func newTestPoint(tag string, value float64) *influx.Point {
	point, err := influx.NewPoint("treasures", map[string]string{"type": tag}, map[string]interface{}{"value": value})
	if err != nil {
		panic(err)
	}
	return point
}

func TestImporter(t *testing.T) {
	initiate()
	resolver := cluster.NewResolver()
	for _, token := range []int{3012244896, 3960162835} {
		resolver.AddToken(token, &cluster.Node{[]int{}, cluster.NodeStatusUp, influxOne.Location, "influx-1"})
	}

	multiple(influxOne, []string{
		"CREATE RETENTION POLICY rp_test ON " + testDB + " DURATION 1h REPLICATION 1",
		"CREATE CONTINUOUS QUERY average_treasure ON " + testDB +
			" BEGIN SELECT mean(value) INTO mean_treasure FROM treasures GROUP BY time(1h) END",
	})

	writePoints([]*influx.Point{
		newTestPoint("gold", 5),
		newTestPoint("silver", 4),
	}, influxOne, testDB, "autogen")

	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(cluster.PartitionKey{Database: testDB, Measurement: "treasures", Tags: []string{"type"}})

	importer := NewImporter(resolver, partitioner, AlwaysPartitionImport)
	importer.ImportPartitioned([]int{3012244896}, influxTwo)

	time.Sleep(10 * time.Millisecond)
	resp, err := influxTwo.Query(influx.NewQuery("SELECT * FROM treasures", testDB, "ns"))
	assert.NoError(t, err)
	assert.Len(t, resp.Results[0].Series[0].Values, 1)
	assert.Equal(t, "silver", resp.Results[0].Series[0].Values[0][1].(string))

	rps, err := influxTwo.ShowRetentionPolicies(testDB)
	assert.Len(t, rps, 2) // autogen + 1

	cqs, err := influxTwo.ShowContinuousQueries(testDB)
	assert.Len(t, cqs, 1)

}
