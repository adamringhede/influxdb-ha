package service

import (
	"time"
	"testing"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
	"encoding/json"
)

func TestQueryHandler_Coordinator_DistributeQueryAndAggregateResults(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select mean(value) from treasures WHERE time <= now() AND (type = 'gold' OR type = 'trash') GROUP BY time(1d) LIMIT 1`)
	assert.Equal(t, json.Number("50"), res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Coordinator_SingleNode(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select mean(value) from treasures WHERE time <= now() AND type = 'gold' GROUP BY time(1d) LIMIT 1`)
	assert.Equal(t, json.Number("100"), res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Coordinator_NoGroupingMultipleNodes(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select value from treasures WHERE time <= now() AND (type = 'gold' OR type = 'silver' OR type = 'trash')`)
	assert.Len(t, res[0].Series[0].Values, 3)
}

func TestQueryHandler_Coordinator_NoGroupingMultipleNodesAggregation(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select mean(value) from treasures WHERE time <= now() AND (type = 'gold' OR type = 'silver' OR type = 'trash')`)
	assert.Len(t, res[0].Series[0].Values, 1)
	assert.Equal(t, json.Number("50"), res[0].Series[0].Values[0][1])
}


func init() {
	clnt1 := newClient(influxOne)
	clnt2 := newClient(influxTwo)
	mustQuery(clnt1, "DROP DATABASE " + testDB)
	mustQuery(clnt1, "CREATE DATABASE " + testDB)
	mustQuery(clnt2, "DROP DATABASE " + testDB)
	mustQuery(clnt2, "CREATE DATABASE " + testDB)
	time.Sleep(10 * time.Millisecond)

	// Simulating correctly partitioned data without replication
	// trash = 1583631877
	// silver = 3042244896
	// gold = 3966162835
	resolver := cluster.NewResolver()
	resolver.ReplicationFactor = 1
	resolver.AddToken(0,  &cluster.Node{[]int{}, cluster.NodeStatusUp, influxOne, "influx-1"})
	resolver.AddToken(3000000000,  &cluster.Node{[]int{}, cluster.NodeStatusUp, influxTwo, "influx-2"})

	writePoints([]*influx.Point{
		newPoint("trash", 0),
	}, clnt1)

	writePoints([]*influx.Point{
		newPoint("gold", 100),
		newPoint("silver", 50),
	}, clnt2)

	partitioner := newPartitioner()

	go Start(resolver, partitioner, Config{
		"0.0.0.0",
		8099,
	})
	time.Sleep(time.Millisecond * 50)
}

func setUpSelectTest() influx.Client {
	return newClient("localhost:8099")
}
