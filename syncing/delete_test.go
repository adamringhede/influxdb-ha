package syncing

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestDelete(t *testing.T) {
	multiple(influxOne, []string{
		"DROP DATABASE " + testDB,
		"CREATE DATABASE " + testDB,
	})
	time.Sleep(10 * time.Millisecond)
	postLines(influxOne, testDB, "autogen", []string{
		"treasures,type=gold,_partitionToken=0 value=5",
		"treasures,type=silver,_partitionToken=100 value=4",
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
