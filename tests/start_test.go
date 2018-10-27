package tests

import (
	"github.com/adamringhede/influxdb-ha/cmd/handle/launcher"
	"github.com/adamringhede/influxdb-ha/service"
	"github.com/adamringhede/influxdb-ha/tests/utils"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/labstack/gommon/random"
	"github.com/stretchr/testify/assert"
	"testing"
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

	node1 := launcher.NewLauncher(clusterId, "node-a", EtcdAddress, utils.InfluxOne, createHttpConfig(8081))
	node1.Join()
	go node1.Listen()

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

	node2.Join()
	node3.Join()

	influxClnt1 := utils.NewClient(utils.InfluxOne)
	influxClnt2 := utils.NewClient(utils.InfluxTwo)
	influxClnt3 := utils.NewClient(utils.InfluxThree)

	assertData(t, influxClnt1, 6)
	assertData(t, influxClnt2, 0)
	assertData(t, influxClnt3, 6)

}
