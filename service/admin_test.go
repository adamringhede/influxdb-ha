package service

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
)

func TestUpdateReplicationFactor(t *testing.T) {
	// write some data, test that it exists on the expected number of hosts.
	// increase replication factor
	// the data should now start to be replicated to other hosts
	// check that data is replicated
	// decrease replication factor
	// make sure it is deleted where it is no longer needed
	// This is not a critical features and we could hold off on it.

	// TODO also need to test updating the default and per database (default should be enough to begin with).
}

func TestShowPartitionKeys(t *testing.T) {
	t.Parallel()
	pks, ch := setupAdminTest()
	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	result := mustQueryClusterAuth(t, ch, "SHOW PARTITION KEYS", "admin:secret")
	assert.Len(t, result[0].Series[0].Values, 1)

	result = mustQueryClusterAuth(t, ch, "SHOW PARTITION KEYS ON test_db", "admin:secret")
	assert.Len(t, result[0].Series[0].Values, 1)

	result = mustQueryClusterAuth(t, ch, "SHOW PARTITION KEYS ON test_db2", "admin:secret")
	assert.Len(t, result[0].Series[0].Values, 0)
}

func TestCreatePartitionKey(t *testing.T) {
	t.Parallel()
	pks, ch := setupAdminTest()
	mustQueryClusterAuth(t, ch, "CREATE PARTITION KEY server_id ON test_db", "admin:secret")

	keys, _ := pks.GetAll()
	assert.Len(t, keys, 1)
	assert.Equal(t, "test_db", keys[0].Database)

	statusCode, _ := mustNotQueryClusterAuth(t, ch, "CREATE PARTITION KEY server_id ON test_db", "admin:secret")
	assert.Equal(t, 409, statusCode)
}

func TestDropPartitionKey(t *testing.T) {
	t.Parallel()
	pks, ch := setupAdminTest()
	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	mustQueryClusterAuth(t, ch, "DROP PARTITION KEY ON test_db.cpu", "admin:secret")

	keys, _ := pks.GetAll()
	assert.Len(t, keys, 0)
}

func TestShowNodes(t *testing.T) {
	t.Parallel()
	_, ch := setupAdminTest()
	ch.nodeStorage.Save(&cluster.Node{Name: "mynode"})
	results := mustQueryClusterAuth(t, ch, "SHOW NODES", "admin:secret")
	assert.Len(t, results[0].Series[0].Values, 1)
}

func TestRemoveNode(t *testing.T) {
	t.Parallel()
	_, ch := setupAdminTest()
	ch.nodeStorage.Save(&cluster.Node{Name: "mynode"})
	mustQueryClusterAuth(t, ch, "REMOVE NODE mynode", "admin:secret")
	node, _ := ch.nodeStorage.Get("mynode")
	assert.Nil(t, node)
}

func TestInvalidQueryFormat(t *testing.T) {
	t.Parallel()
	_, ch := setupAdminTest()
	statusCode, msg := mustNotQueryClusterAuth(t, ch, "DROP PARTITION", "admin:secret")
	assert.Equal(t, statusCode, 400)
	assert.Equal(t, `{"error":"error parsing query: unexpected end of statement, expecting KEY"}`, msg)
}

func setupAdminTest() (cluster.PartitionKeyStorage, *ClusterHandler) {
	pks := NewMockedPartitionKeyStorage()
	ns := NewMockedNodeStorage()
	authStorage := cluster.NewMockAuthStorage()
	authService := NewPersistentAuthService(authStorage)
	authService.CreateUser(cluster.UserInfo{Name: "admin", Hash: cluster.HashUserPassword("secret"), Admin: true})
	authService.Save()

	ch := &ClusterHandler{partitionKeyStorage: pks, nodeStorage: ns, authService: authService}
	return pks, ch
}

func mustNotQueryCluster(t *testing.T, handler http.Handler, cmd string) (int, string) {
	return mustNotQueryClusterAuth(t, handler, cmd, "")
}

func mustNotQueryClusterAuth(t *testing.T, handler http.Handler, cmd string, auth string) (int, string) {
	resp := _execClusterCommand(handler, cmd, auth)
	assert.NotEqual(t, resp.StatusCode, 200)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return resp.StatusCode, strings.TrimRight(string(body), "\n")
}

func mustQueryCluster(t *testing.T, handler http.Handler, cmd string) []Result {
	return mustQueryClusterAuth(t, handler, cmd, "")
}

func mustQueryClusterAuth(t *testing.T, handler http.Handler, cmd string, auth string) []Result {
	resp := _execClusterCommand(handler, cmd, auth)
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, 200, resp.StatusCode, "Body: %s", string(body))
	}
	return parseResp(resp.Body, false).Results
}

func _execClusterCommand(handler http.Handler, cmd string, auth string) *http.Response {
	req := httptest.NewRequest("GET", fmt.Sprintf("http://%s@localhost/query?q=%s&db=%s&epoch=ns", auth, url.QueryEscape(cmd), testDB), nil)
	if auth != "" {
		req.SetBasicAuth(strings.Split(auth, ":")[0], strings.Split(auth, ":")[1])
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
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

func (s *MockedNodeStorage) OnRemove(h func(cluster.Node)) {

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
