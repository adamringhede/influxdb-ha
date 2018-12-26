package syncing

import (
	influx "github.com/influxdata/influxdb/client/v2"

	"testing"
	"time"
)

func TestGetSeriesByPartitionKey(t *testing.T) {
	multiple(influxOne, []string{
		"DROP DATABASE " + testDB,
		"CREATE DATABASE " + testDB,
	})

	time.Sleep(1000 * time.Millisecond)

	writePoints([]*influx.Point{
    		newTestPoint("gold", 5),
    		newTestPoint("silver", 4),
    		newTestPoint("trash", 4),
            newTestPoint("foo", 4),
    	}, influxOne, testDB, "autogen")

	time.Sleep(1000 * time.Millisecond)
	FetchSeries(influxOne, testDB)
}