package syncing

import (
	influx "github.com/influxdata/influxdb/client/v2"
	"testing"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
)

func setUpReliableTest() (*clientv3.Client, *cluster.Resolver) {
	initiate()
	resolver := cluster.NewResolver()
	for _, token := range []int{0, 100} {
		resolver.AddToken(token, &cluster.Node{[]int{}, cluster.NodeStatusUp, influxOne.Location, "influx-1"})
	}

	writePoints([]*influx.Point{
		newTestPoint("gold", 5),
		newTestPoint("silver", 4),
	}, influxOne, testDB, "autogen")
	time.Sleep(500 * time.Millisecond)

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + etcdLoc},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return etcdClient, resolver
}

func TestReliableImporter(t *testing.T) {
	etcdClient, resolver := setUpReliableTest()

	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(cluster.PartitionKey{Database: testDB, Measurement: "treasures", Tags: []string{"type"}})
	importer := NewImporter(resolver, partitioner, func(db, msmt string) ImportDecision {
		return PartitionImport
	})

	wq := cluster.NewEtcdWorkQueue(etcdClient, "local", "import")
	wq.Clear()
	// Add a task to be picked up when starting
	wq.Push("local", ReliableImportPayload{Tokens: []int{0}})
	reliable := NewReliableImporter(importer, wq, resolver, influxTwo)

	workc := wq.Subscribe()
	task1 := <-workc

	var payload ReliableImportPayload
	var checkpoint ReliableImportCheckpoint
	task1.Unmarshal(&payload, &checkpoint)

	reliable.process(task1.ID, payload, checkpoint)

	// Add task when running
	wq.Push("local", ReliableImportPayload{Tokens: []int{100}})

	task2 := <-workc

	var payload2 ReliableImportPayload
	var checkpoint2 ReliableImportCheckpoint
	task2.Unmarshal(&payload2, &checkpoint2)

	reliable.process(task2.ID, payload2, checkpoint2)

	// Give time to allow it to import
	time.Sleep(100 * time.Millisecond)

	resp, err := influxTwo.Query(influx.NewQuery("SELECT * FROM treasures", testDB, "ns"))
	assert.NoError(t, err)
	assert.Len(t, resp.Results[0].Series[0].Values, 2)
	assert.Equal(t, "gold", resp.Results[0].Series[0].Values[0][1].(string))
}
