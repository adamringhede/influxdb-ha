package tests

import (
	"context"
	"github.com/adamringhede/influxdb-ha/cmd/handle/launcher"
	"github.com/adamringhede/influxdb-ha/service"
	"github.com/adamringhede/influxdb-ha/tests/utils"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/labstack/gommon/random"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const EtcdAddress = "192.168.99.100:2379"

func createHttpConfig(port int) service.Config {
	return service.Config{
		BindAddr: "0.0.0.0",
		BindPort: port,
	}
}

func TestStart(t *testing.T) {
	clearInflux()

	// Create a random cluster id to separate the new nodes from old data that may exist in Etcd.
	clusterId := random.New().String(16, random.Hex)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node1 := launcher.NewLauncher(clusterId, "node-a", EtcdAddress, utils.InfluxOne, createHttpConfig(8081))
	node1.Join()
	go node1.Listen(ctx)

	clnt1 := utils.NewClient("localhost:8081")
	utils.MustQuery(clnt1, "CREATE USER admin WITH PASSWORD 'password' WITH ALL PRIVILEGES")
	utils.WritePoints([]*influx.Point{
		utils.NewPoint("a", 2),
		utils.NewPoint("b", 2),
		utils.NewPoint("c", 2),
		utils.NewPoint("d", 2),
		utils.NewPoint("e", 2),
		utils.NewPoint("f", 2),
	}, clnt1)

	node2 := launcher.NewLauncher(clusterId, "node-b", EtcdAddress, utils.InfluxTwo, createHttpConfig(8082))
	node3 := launcher.NewLauncher(clusterId, "node-c", EtcdAddress, utils.InfluxThree, createHttpConfig(8083))

	assert.True(t, node1.IsNew)
	assert.True(t, node2.IsNew)
	assert.True(t, node3.IsNew)

	assert.NoError(t, node2.Join())
	assert.NoError(t, node3.Join())

	influxClnt1 := utils.NewClient(utils.InfluxOne)
	influxClnt2 := utils.NewClient(utils.InfluxTwo)
	influxClnt3 := utils.NewClient(utils.InfluxThree)

	assertData(t, influxClnt1, 0)
	assertData(t, influxClnt2, 6)
	assertData(t, influxClnt3, 6)

}

func TestStartWithPartitioned(t *testing.T) {
	clearInflux()
	time.Sleep(1 * time.Second)

	// Create a random cluster id to separate the new nodes from old data that may exist in Etcd.
	clusterId := random.New().String(16, random.Hex)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node1 := launcher.NewLauncher(clusterId, "node-a", EtcdAddress, utils.InfluxOne, createHttpConfig(8081))
	assert.NoError(t, node1.Join())
	go node1.Listen(ctx)

	clnt1 := utils.NewClient("localhost:8081")
	utils.MustQuery(clnt1, "CREATE USER admin WITH PASSWORD 'password' WITH ALL PRIVILEGES")
	utils.MustQuery(clnt1, "CREATE PARTITION KEY type ON sharded.treasures")

	points := make([]*influx.Point, 10000)
	for i := range points {
		points[i] = utils.NewPoint(random.New().String(16, random.Hex), 2)
	}

	utils.WritePoints(points, clnt1)
	time.Sleep(1000 * time.Millisecond)

	node2 := launcher.NewLauncher(clusterId, "node-b", EtcdAddress, utils.InfluxTwo, createHttpConfig(8082))
	node3 := launcher.NewLauncher(clusterId, "node-c", EtcdAddress, utils.InfluxThree, createHttpConfig(8083))

	assert.True(t, node1.IsNew)
	assert.True(t, node2.IsNew)
	assert.True(t, node3.IsNew)

	assert.NoError(t, node2.Join())
	assert.NoError(t, node3.Join())

	influxClnt1 := utils.NewClient(utils.InfluxOne)
	influxClnt2 := utils.NewClient(utils.InfluxTwo)
	influxClnt3 := utils.NewClient(utils.InfluxThree)

	assertData(t, influxClnt1, 25)
	assertData(t, influxClnt2, 7)
	assertData(t, influxClnt3, 32)
}