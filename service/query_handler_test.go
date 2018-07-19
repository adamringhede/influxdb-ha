package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	)

func TestQueryHandler_Coordinator_DistributeQueryAndAggregateResults(t *testing.T) {
	handler := setUpSelectTest()
	res := mustQueryCluster(t, handler, `select mean(value) from treasures WHERE time <= now() AND (type = 'gold' OR type = 'trash') GROUP BY time(1d) LIMIT 1`)
	assert.Equal(t, 50., res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Coordinator_SingleNode(t *testing.T) {
	handler := setUpSelectTest()
	res := mustQueryCluster(t, handler, `select mean(value) from treasures WHERE time <= now() AND type = 'gold' GROUP BY time(1d) LIMIT 1`)
	assert.Equal(t, 100., res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Coordinator_NoGroupingMultipleNodes(t *testing.T) {
	handler := setUpSelectTest()
	res := mustQueryCluster(t, handler, `select value from treasures WHERE time <= now() AND (type = 'gold' OR type = 'silver' OR type = 'trash')`)
	assert.Len(t, res[0].Series[0].Values, 3)
}

func TestQueryHandler_Coordinator_NoGroupingMultipleNodesAggregation(t *testing.T) {
	handler := setUpSelectTest()
	res := mustQueryCluster(t, handler, `select mean(value) from treasures WHERE time <= now() AND (type = 'gold' OR type = 'silver' OR type = 'trash')`)
	assert.Len(t, res[0].Series[0].Values, 1)
	assert.Equal(t, 50., res[0].Series[0].Values[0][1])
}

func TestQueryHandler_Admin_GrandAdmin(t *testing.T) {
	storage := cluster.NewMockAuthStorage()
	handler := NewQueryHandler(newTestResolver(), newPartitioner(),
		nil, NewPersistentAuthService(storage))

	// The first request has to be creating the admin user
	mustNotQueryCluster(t, handler, `SHOW DATABASES`)

	// Creating an admin user should work
	mustQueryCluster(t, handler, `CREATE USER admin WITH PASSWORD 'password' WITH ALL PRIVILEGES`)

	// But then it should stop working
	mustNotQueryCluster(t, handler, `CREATE USER admin2 WITH PASSWORD 'password' WITH ALL PRIVILEGES`)

	// Authentication should work now.
	mustQueryClusterAuth(t, handler, `SHOW USERS`, "admin:password")
	mustQueryClusterAuth(t, handler, `CREATE USER adam WITH PASSWORD 'password'`, "admin:password")

	// The user should not be able to show users without any privileges
	mustNotQueryClusterAuth(t, handler, `SHOW USERS`, "adam:password")

	// Grant the user all privileges
	mustQueryClusterAuth(t, handler, `GRANT ALL PRIVILEGES TO "adam"`, "admin:password")

	// Now it should work
	mustQueryClusterAuth(t, handler, `SHOW USERS`, "adam:password")

	_, err := storage.Get()
	assert.NoError(t, err)
}

func newTestResolver() *cluster.Resolver {
	resolver := cluster.NewResolver()
	resolver.ReplicationFactor = 1
	resolver.AddToken(0, &cluster.Node{[]int{}, cluster.NodeStatusUp, influxOne, "influx-1"})
	resolver.AddToken(3000000000, &cluster.Node{[]int{}, cluster.NodeStatusUp, influxTwo, "influx-2"})
	return resolver
}

func setup() {
	clnt1 := newClient(influxOne)
	clnt2 := newClient(influxTwo)
	mustQuery(clnt1, "DROP DATABASE "+testDB)
	mustQuery(clnt1, "CREATE DATABASE "+testDB)
	mustQuery(clnt2, "DROP DATABASE "+testDB)
	mustQuery(clnt2, "CREATE DATABASE "+testDB)
	time.Sleep(10 * time.Millisecond)

	// Simulating correctly partitioned data without replication
	// trash = 1583631877
	// silver = 3042244896
	// gold = 3966162835

	writePoints([]*influx.Point{
		newPoint("trash", 0),
	}, clnt1)

	writePoints([]*influx.Point{
		newPoint("gold", 100),
		newPoint("silver", 50),
	}, clnt2)

	// TODO We don't need to start the server. We only need to test the handlers.
	/*go Start(resolver, partitioner, cluster.NewLocalRecoveryStorage("./", nil), pks, nil, nil, Config{
		"0.0.0.0",
		8099,
	})*/
	time.Sleep(time.Millisecond * 50)
}

func setUpSelectTest() *QueryHandler {
	setup()
	handler := NewQueryHandler(newTestResolver(), newPartitioner(),
		nil, nil)
	return handler
}
