package service

import (
	"time"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/adamringhede/influxdb-ha/cluster"
	"log"
)


const influxOne = "192.168.99.100:28086"
const influxTwo = "192.168.99.100:27086"
const influxThree = "192.168.99.100:26086"
const testDB = "sharded"

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
		time.Now().AddDate(0, 0, -1),
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
