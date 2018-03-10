package syncing

import (
	"testing"

	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
)

const influxOne = "192.168.99.100:28086"
const influxTwo = "192.168.99.100:27086"
const influxThree = "192.168.99.100:26086"
const etcdLoc = "192.168.99.100:2379"
const testDB = "sharded"

func multiple(location string, commands []string) {
	for _, q := range commands {
		resp, err := get(q, location, "", false)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != 200 {
			panic("Status code is not 200")
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
	time.Sleep(50 * time.Millisecond)
}

func Test_fetchLocationMeta(t *testing.T) {
	initiate()

	meta, err := fetchLocationMeta(influxOne)
	assert.Contains(t, meta.databases, testDB)
	assert.NoError(t, err)
}

func TestImporter(t *testing.T) {
	initiate()
	resolver := cluster.NewResolver()
	for _, token := range []int{3012244896, 3960162835} {
		resolver.AddToken(token, &cluster.Node{[]int{}, cluster.NodeStatusUp, influxOne, "influx-1"})
	}

	postLines(influxOne, testDB, "autogen", []string{
		"treasures,type=gold," + cluster.PartitionTagName + "=3966162835 value=5",
		"treasures,type=silver," + cluster.PartitionTagName + "=3042244896 value=4",
	})

	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(cluster.PartitionKey{Database: testDB, Measurement: "treasures", Tags: []string{"type"}})

	importer := NewImporter(resolver, partitioner, AlwaysPartitionImport)
	importer.Import([]int{3012244896}, resolver, influxTwo)

	results, err := fetchSimple("SELECT * FROM treasures", influxTwo, testDB)
	assert.NoError(t, err)
	assert.Len(t, results[0].Series[0].Values, 1)
	assert.Equal(t, "silver", results[0].Series[0].Values[0][2].(string))
}
