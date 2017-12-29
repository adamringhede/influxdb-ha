package syncing

import (
	"testing"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/coreos/etcd/clientv3"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
)

type PausingWorkQueue struct {
	cluster.MockedWorkQueue
}

func (wq *PausingWorkQueue) CheckIn(task cluster.Task) {
	wq.MockedWorkQueue.CheckIn(task)
	time.Sleep(10 * time.Millisecond)
}

func setUpReliableTest() (*clientv3.Client, *cluster.Resolver) {
	initiate()
	resolver := cluster.NewResolver()
	for _, token := range []int{0, 100} {
		resolver.AddToken(token, &cluster.Node{[]int{}, cluster.NodeStatusUp, influxOne, "influx-1"})
	}

	postLines(influxOne, testDB, "autogen", []string{
		"treasures,type=gold," + cluster.PartitionTagName + "=0 value=5",
		"treasures,type=silver," + cluster.PartitionTagName + "=100 value=4",
	})

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + etcdLoc},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return etcdClient, resolver
}

/*
	Test that
*/
func TestReliableImporter(t *testing.T) {
	etcdClient, resolver := setUpReliableTest()

	importer := &BasicImporter{}

	wq := cluster.NewEtcdWorkQueue(etcdClient, "local", "import")
	wq.Clear()
	// Add a task to be picked up when starting
	wq.Push("local", ReliableImportPayload{[]int{0}})
	reliable := NewReliableImporter(importer, wq, resolver, influxTwo)

	workc := wq.Subscribe()
	task1 := <-workc
	reliable.process(task1)

	// Add task when running
	wq.Push("local", ReliableImportPayload{[]int{100}})

	task2 := <-workc
	reliable.process(task2)

	// Give time to allow it to import
	time.Sleep(2000 * time.Millisecond)

	results, err := fetchSimple("SELECT * FROM treasures", influxTwo, testDB)
	assert.NoError(t, err)
	spew.Dump(results)
	assert.Len(t, results[0].Series[0].Values, 2)
	assert.Equal(t, "gold", results[0].Series[0].Values[0][2].(string))
}
