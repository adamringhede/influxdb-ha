package tests

import (
	"testing"

	"github.com/adamringhede/influxdb-ha/tests/utils"
	influx "github.com/influxdata/influxdb/client/v2"
	"time"
)

func TestRemoveNode(t *testing.T) {
	handle := utils.NewClient("192.168.99.100:8086")

	utils.WritePoints([]*influx.Point{
		utils.NewPoint("trash", 10),
	}, handle)

	clnt1 := utils.NewClient(utils.InfluxOne)
	clnt2 := utils.NewClient(utils.InfluxTwo)
	clnt3 := utils.NewClient(utils.InfluxThree)

	assertData(t, clnt1, 1)
	assertData(t, clnt2, 0)
	assertData(t, clnt3, 1)

	utils.StopNode(utils.Nodes[2])
	utils.MustQuery(handle, "REMOVE NODE " + utils.Nodes[0][0])
	// TODO try removing it before stopping.

	time.Sleep(2 * time.Second)

	// Tokens should now have been distributed and the data should exist on all existing nodes.
	assertData(t, clnt1, 1)
	assertData(t, clnt2, 1)
}

func TestRemoveNodeWhenOthersAreDown(t *testing.T) {
	handle := utils.NewClient("192.168.99.100:8086")

	utils.WritePoints([]*influx.Point{
		utils.NewPoint("trash", 10),
	}, handle)

	clnt1 := utils.NewClient(utils.InfluxOne)
	clnt2 := utils.NewClient(utils.InfluxTwo)
	clnt3 := utils.NewClient(utils.InfluxThree)

	assertData(t, clnt1, 1)
	assertData(t, clnt2, 0)
	assertData(t, clnt3, 1)

	utils.StopNode(utils.Nodes[2])
}