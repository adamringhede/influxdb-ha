package tests

import (
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"

	"github.com/adamringhede/influxdb-ha/tests/utils"
	"github.com/stretchr/testify/assert"
	"time"
)

// TestRecovery tests that data not written to the stopped node will then be rewritten when that node becomes alive.
func TestRecovery(t *testing.T) {
	handle := utils.NewClient("192.168.99.100:8086")

	// write against the handle, not the db.
	utils.WritePoints([]*influx.Point{
		utils.NewPoint("trash", 2),
	}, handle)

	clnt1 := utils.NewClient(utils.InfluxOne)
	clnt2 := utils.NewClient(utils.InfluxTwo)

	for _, clnt := range []influx.Client{clnt1, clnt2} {
		assertData(t, clnt, 1)
	}

	utils.StopNode(utils.Nodes[1])

	utils.WritePoints([]*influx.Point{
		utils.NewPoint("trash", 10),
	}, handle)

	// the data is only written to one of the nodes
	time.Sleep(time.Millisecond * 50)
	assertData(t, clnt1, 2)

	utils.StartNode(utils.Nodes[1])

	// Wait some time for recovering to work
	time.Sleep(time.Millisecond * 2000)

	// It should now be recovered.
	assertData(t, clnt2, 2)
}

func assertData(t *testing.T, clnt influx.Client, count int) {
	res := utils.MustQuery(clnt, `select value from treasures`)
	if count > 0 && assert.Len(t, res[0].Series, 1) {
		assert.Len(t, res[0].Series[0].Values, count)
	} else {
		assert.Len(t, res[0].Series, 0)
	}

}

func init() {
	for _, node := range utils.Nodes {
		utils.StartNode(node)
	}
	time.Sleep(time.Millisecond * 200)
	clnt1 := utils.NewClient(utils.InfluxOne)
	clnt2 := utils.NewClient(utils.InfluxTwo)
	clnt3 := utils.NewClient(utils.InfluxThree)
	utils.MustQuery(clnt1, "DROP DATABASE " + utils.TestDB)
	utils.MustQuery(clnt1, "CREATE DATABASE " + utils.TestDB)
	utils.MustQuery(clnt2, "DROP DATABASE " + utils.TestDB)
	utils.MustQuery(clnt2, "CREATE DATABASE " + utils.TestDB)
	utils.MustQuery(clnt3, "DROP DATABASE " + utils.TestDB)
	utils.MustQuery(clnt3, "CREATE DATABASE " + utils.TestDB)
	time.Sleep(time.Millisecond * 50)
}
