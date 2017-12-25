package service

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
)

func TestShowPartitionKeys(t *testing.T) {
	t.Parallel()
	pks, ch := setupAdminTest()
	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	result := mustQueryCluster(t, ch, "SHOW PARTITION KEYS")
	assert.Len(t, result[0].Series[0].Values, 1)

	result = mustQueryCluster(t, ch, "SHOW PARTITION KEYS ON test_db")
	assert.Len(t, result[0].Series[0].Values, 1)

	result = mustQueryCluster(t, ch, "SHOW PARTITION KEYS ON test_db2")
	assert.Len(t, result[0].Series[0].Values, 0)
}

func TestCreatePartitionKey(t *testing.T) {
	t.Parallel()
	pks, ch := setupAdminTest()
	mustQueryCluster(t, ch, "CREATE PARTITION KEY server_id ON test_db")

	keys, _ := pks.GetAll()
	assert.Len(t, keys, 1)
	assert.Equal(t, "test_db", keys[0].Database)

	statusCode, _ := mustNotQueryCluster(t, ch, "CREATE PARTITION KEY server_id ON test_db")
	assert.Equal(t, 409, statusCode)
}

func TestDropPartitionKey(t *testing.T) {
	t.Parallel()
	pks, ch := setupAdminTest()
	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	mustQueryCluster(t, ch, "DROP PARTITION KEY ON test_db.cpu")

	keys, _ := pks.GetAll()
	assert.Len(t, keys, 0)
}

func TestShowNodes(t *testing.T) {
	t.Parallel()
	_, ch := setupAdminTest()
	ch.nodeStorage.Save(&cluster.Node{Name: "mynode"})
	results := mustQueryCluster(t, ch, "SHOW NODES")
	assert.Len(t, results[0].Series[0].Values, 1)
}

func TestRemoveNode(t *testing.T) {
	t.Parallel()
	_, ch := setupAdminTest()
	ch.nodeStorage.Save(&cluster.Node{Name: "mynode"})
	mustQueryCluster(t, ch, "REMOVE NODE mynode")
	node, _ := ch.nodeStorage.Get("mynode")
	assert.Nil(t, node)
}

func TestInvalidQueryFormat(t *testing.T) {
	t.Parallel()
	_, ch := setupAdminTest()
	statusCode, msg := mustNotQueryCluster(t, ch, "DROP PARTITION")
	assert.Equal(t, statusCode, 400)
	assert.Equal(t, `{"error":"error parsing query: unexpected end of statement, expecting KEY"}`, msg)
}

func setupAdminTest() (cluster.PartitionKeyStorage, *ClusterHandler) {
	pks := NewMockedPartitionKeyStorage()
	ns := NewMockedNodeStorage()
	ch := &ClusterHandler{partitionKeyStorage: pks, nodeStorage: ns}
	return pks, ch
}

func mustNotQueryCluster(t *testing.T, ch *ClusterHandler, cmd string) (int, string) {
	resp := _execClusterCommand(ch, cmd)
	assert.NotEqual(t, resp.StatusCode, 200)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return resp.StatusCode, strings.TrimRight(string(body), "\n")
}

func mustQueryCluster(t *testing.T, ch *ClusterHandler, cmd string) []Result {
	resp := _execClusterCommand(ch, cmd)
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, 200, resp.StatusCode, "Body: %s", string(body))
	}
	return parseResp(resp.Body, false).Results
}

func _execClusterCommand(ch *ClusterHandler, cmd string) *http.Response {
	req := httptest.NewRequest("GET", "http://localhost?q="+url.QueryEscape(cmd), nil)
	w := httptest.NewRecorder()
	ch.ServeHTTP(w, req)
	return w.Result()
}

type MockedNodeStorage struct {
	nodes []*cluster.Node
}

func (ns *MockedNodeStorage) Remove(name string) (bool, error) {
	for i, node := range ns.nodes {
		if node.Name == name {
			ns.nodes = append(ns.nodes[:i], ns.nodes[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

func (ns *MockedNodeStorage) GetAll() ([]*cluster.Node, error) {
	return ns.nodes, nil
}

func (ns *MockedNodeStorage) Get(name string) (*cluster.Node, error) {
	for _, node := range ns.nodes {
		if node.Name == name {
			return node, nil
		}
	}
	return nil, nil
}

func (ns *MockedNodeStorage) Save(node *cluster.Node) error {
	if existing, _ := ns.Get(node.Name); existing == nil {
		ns.nodes = append(ns.nodes, node)
	}
	return nil
}

func NewMockedNodeStorage() *MockedNodeStorage {
	return &MockedNodeStorage{[]*cluster.Node{}}
}

type MockedPartitionKeyStorage struct {
	storage []*cluster.PartitionKey
}

func NewMockedPartitionKeyStorage() *MockedPartitionKeyStorage {
	return &MockedPartitionKeyStorage{[]*cluster.PartitionKey{}}
}

func (s *MockedPartitionKeyStorage) Watch() clientv3.WatchChan {
	return make(<-chan clientv3.WatchResponse)
}

func (s *MockedPartitionKeyStorage) Save(partitionKey *cluster.PartitionKey) error {
	s.storage = append(s.storage, partitionKey)
	return nil
}

func (s *MockedPartitionKeyStorage) Drop(db, msmt string) error {
	for i, pk := range s.storage {
		if pk.Database == db && pk.Measurement == msmt {
			s.storage[i] = nil
			s.storage = append(s.storage[:i], s.storage[i+1:]...)
		}
	}
	return nil
}

func (s *MockedPartitionKeyStorage) GetAll() ([]*cluster.PartitionKey, error) {
	return s.storage, nil
}

func startServer() {
	resolver := cluster.NewResolver()
	partitioner := cluster.NewPartitioner()
	pks := NewMockedPartitionKeyStorage()

	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	go Start(resolver, partitioner, cluster.NewLocalRecoveryStorage("./", nil), pks, nil, Config{
		"0.0.0.0",
		8099,
	})
}
