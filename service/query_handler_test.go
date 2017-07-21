package service

import (
	"time"
	"testing"
	influx "github.com/influxdata/influxdb/client/v2"
	"log"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
	"encoding/json"
)

const influxOne = "192.168.99.100:28086"
const influxTwo = "192.168.99.100:27086"
const influxThree = "192.168.99.100:26086"
const testDB = "sharded"

func TestQueryHandler_Coordinator_DistributeQueryAndAggregateResults(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select mean(value) from treasures WHERE time <= now() AND type = 'gold' OR type = 'trash' GROUP BY time(1d)`)
	assert.Equal(t, json.Number("50"), res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Coordinator_SingleNode(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select mean(value) from treasures WHERE time <= now() AND type = 'gold' GROUP BY time(1d)`)
	assert.Equal(t, json.Number("100"), res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Coordinator_NoGroupingMultipleNodes(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select value from treasures WHERE time <= now() AND type = 'gold' OR type = 'silver' OR type = 'trash'`)
	assert.Len(t, res[0].Series[0].Values, 3)
}

func TestQueryHandler_Coordinator_NoGroupingMultipleNodesAggregation(t *testing.T) {
	clnt := setUpSelectTest()
	res := mustQuery(clnt, `select mean(value) from treasures WHERE time <= now() AND (type = 'gold' OR type = 'silver' OR type = 'trash')`)
	assert.Len(t, res[0].Series[0].Values, 1)
	assert.Equal(t, json.Number("50"), res[0].Series[0].Values[0][1])
}

func newClient(location string) influx.Client {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: "http://" + location,
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func query(clnt influx.Client, cmd string) (res []influx.Result, err error) {
	q := influx.Query{
		Command:  cmd,
		Database: testDB,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func mustQuery(clnt influx.Client, cmd string) []influx.Result {
	res, err := query(clnt, cmd)
	if err != nil {
		log.Panic(err)
	}
	return res
}

func getPartitionKey() cluster.PartitionKey {
	return cluster.PartitionKey{"sharded", "treasures", []string{"type"}}
}

func newPartitioner() *cluster.Partitioner {
	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(getPartitionKey())
	return partitioner
}

func newPoint(tag string, value float64) *influx.Point {
	pt, err := influx.NewPoint(
		"treasures",
		map[string]string{
			"type": tag,
		},
		map[string]interface{}{
			"value": value,
		},
		time.Now(),
	)
	if err != nil {
		log.Fatal(err)
	}
	return pt
}

func writePoints(points []*influx.Point, clnt influx.Client) {
	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database: testDB,
		Precision: "us",
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, pt := range points {
		bp.AddPoint(pt)
	}
	clnt.Write(bp)
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
	resolver.AddToken(0,  &cluster.Node{[]int{}, cluster.STATUS_UP, influxOne, "influx-1"})
	resolver.AddToken(3000000000,  &cluster.Node{[]int{}, cluster.STATUS_UP, influxTwo, "influx-2"})

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
