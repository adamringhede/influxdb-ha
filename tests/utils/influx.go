package utils

import (
	"log"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	influx "github.com/influxdata/influxdb/client/v2"
)

const InfluxOne = "192.168.99.100:28086"
const InfluxTwo = "192.168.99.100:27086"
const InfluxThree = "192.168.99.100:26086"
const TestDB = "sharded"

func NewClient(location string) influx.Client {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: "http://" + location,
		Username: "admin",
		Password: "password",
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func Query(clnt influx.Client, cmd string) (res []influx.Result, err error) {
	q := influx.Query{
		Command:  cmd,
		Database: TestDB,
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

func MustQuery(clnt influx.Client, cmd string) []influx.Result {
	res, err := Query(clnt, cmd)
	if err != nil {
		log.Panic(err)
	}
	return res
}

func GetPartitionKey() cluster.PartitionKey {
	return cluster.PartitionKey{"sharded", "treasures", []string{"type"}}
}

func NewPartitioner() cluster.Partitioner {
	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(GetPartitionKey())
	return partitioner
}

func NewPoint(tag string, value float64) *influx.Point {

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

func WritePoints(points []*influx.Point, clnt influx.Client) {
	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:        TestDB,
		RetentionPolicy: "autogen",
		Precision:       "us",
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, pt := range points {
		bp.AddPoint(pt)
	}
	err = clnt.Write(bp)
	if err != nil {
		panic(err)
	}
}
