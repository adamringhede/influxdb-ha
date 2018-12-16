package tests

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"strings"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"

	"time"

	"github.com/adamringhede/influxdb-ha/tests/utils"
	"github.com/stretchr/testify/assert"
)

// TestRecovery tests that data not written to the stopped node will then be rewritten when that node becomes alive.
// Note that this may fail if the distribution of tokens changes as that will result in changing
// which nodes will receive data.
func TestRecovery(t *testing.T) {
	initWithNodes()

	handle := utils.NewClient("127.0.0.1:8086")

	// write against the handle, not the db.
	utils.WritePoints([]*influx.Point{
		utils.NewPoint("trash", 2),
	}, handle)

	clnt1 := utils.NewClient(utils.InfluxTwo)
	clnt2 := utils.NewClient(utils.InfluxThree)

	for _, clnt := range []influx.Client{clnt1, clnt2} {
		assertData(t, clnt, 1)
	}

	utils.StopNode(utils.Nodes[2])

	utils.WritePoints([]*influx.Point{
		utils.NewPoint("trash", 10),
	}, handle)

	// the data is only written to one of the nodes
	time.Sleep(time.Millisecond * 50)
	assertData(t, clnt1, 2)

	utils.StartNode(utils.Nodes[2])

	// Wait some time for recovering to work
	time.Sleep(time.Millisecond * 3000)

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

func clearInflux() {
	clnt1 := utils.NewClient(utils.InfluxOne)
	clnt2 := utils.NewClient(utils.InfluxTwo)
	clnt3 := utils.NewClient(utils.InfluxThree)
	for _, clnt := range []influx.Client{clnt1, clnt2, clnt3} {
		utils.MustQuery(clnt, "DROP DATABASE "+utils.TestDB)
		utils.MustQuery(clnt, "CREATE DATABASE "+utils.TestDB)
	}
	time.Sleep(time.Millisecond * 50)
}

func clearEtcd() {
	c, connectErr := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(EtcdAddress, ","),
		DialTimeout: 2 * time.Second,
	})
	if connectErr != nil {
		panic(connectErr)
	}
	_, err := c.Delete(context.Background(), "", clientv3.WithPrefix())
	if err != nil {
		panic(err)
	}
}

func initWithNodes() {
	for _, node := range utils.Nodes {
		utils.StopNode(node)
	}
	clearEtcd()
	for _, node := range utils.Nodes {
		utils.StartNode(node)
	}
	clearInflux()
	time.Sleep(time.Second)

	handle := utils.NewClient("127.0.0.1:8086")
	utils.Query(handle, "CREATE USER admin WITH PASSWORD 'password' WITH ALL PRIVILEGES")
	time.Sleep(time.Second)
	utils.MustQuery(handle, "SHOW USERS")
	utils.Query(handle, "DROP PARTITION KEY ON sharded.treasures")
	time.Sleep(time.Second)
	utils.MustQuery(handle, "CREATE PARTITION KEY type ON sharded.treasures")
}
