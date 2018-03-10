package syncing

import (
	"testing"
	"time"
)

func TestGetSeriesByPartitionKey(t *testing.T) {
	multiple(influxOne, []string{
		"DROP DATABASE " + testDB,
		"CREATE DATABASE " + testDB,
	})

	time.Sleep(1000 * time.Millisecond)

	postLines(influxOne, testDB, "autogen", []string{
		"treasures,type=gold,captain=foo value=5",
		"treasures,type=silver,captain=bar value=4",
		"treasures,type=trash value=4",
		"treasures,captain=foo value=4",
	})

	time.Sleep(1000 * time.Millisecond)
	FetchSeries(influxOne, testDB)
}