package syncing

import (
	"testing"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	multiple(influxOne, []string{
		"DROP DATABASE " + testDB,
		"CREATE DATABASE " + testDB,
	})
	time.Sleep(10 * time.Millisecond)
	postLines(influxOne, testDB, "autogen", []string{
		"treasures,type=gold," + cluster.PartitionTagName + "=0 value=5",
		"treasures,type=silver," + cluster.PartitionTagName + "=100 value=4",
	})

	assertPointsCount(t, 2)
	Delete(100, influxOne)
	assertPointsCount(t, 1)
	Delete(0, influxOne)
	assertPointsCount(t, 0)
}

func assertPointsCount(t *testing.T, count int) {
	results, err := fetchSimple("SELECT * FROM treasures", influxOne, testDB)
	assert.NoError(t, err)
	if count == 0 {
		assert.Empty(t, results[0].Series)
	} else {
		assert.Len(t, results[0].Series[0].Values, count)
	}

}
