package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/coreos/etcd/clientv3"
	"net/http/httptest"
	"net/url"
	"net/http"
	"io/ioutil"
	"strings"
)


func TestShowPartitionKeys(t *testing.T) {
	pks, ch := setupAdminTest()
	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	result := mustQueryCluster(t, ch, "SHOW PARTITION KEYS")
	assert.Len(t, result[0].Series[0].Values, 1)
}

func TestDropPartitionKey(t *testing.T) {
	pks, ch := setupAdminTest()
	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	mustQueryCluster(t, ch, "DROP PARTITION KEY ON test_db.cpu")

	keys, _ := pks.GetAll()
	assert.Len(t, keys, 0)
}

func TestInvalidQueryFormat(t *testing.T) {
	_, ch := setupAdminTest()
	statusCode, msg := mustNotQueryCluster(t, ch,"DROP PARTITION")
	assert.Equal(t, statusCode, 400)
	assert.Equal(t, `{"error":"error parsing query: unexpected end of statement, expecting KEY"}`, msg)
}

func setupAdminTest() (cluster.PartitionKeyStorage, *ClusterHandler) {
	pks := NewMockedPartitionKeyStorage()
	ch := &ClusterHandler{pks}
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
	assert.Equal(t, 200, resp.StatusCode)
	return parseResp(resp.Body, false).Results
}

func _execClusterCommand(ch *ClusterHandler, cmd string) *http.Response {
	req := httptest.NewRequest("GET", "http://localhost?q=" + url.QueryEscape(cmd), nil)
	w := httptest.NewRecorder()
	ch.ServeHTTP(w, req)
	return w.Result()
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
	partitioner :=  cluster.NewPartitioner()
	pks := NewMockedPartitionKeyStorage()

	pks.Save(&cluster.PartitionKey{"test_db", "cpu", []string{"server_id"}})

	go Start(resolver, partitioner, cluster.NewLocalRecoveryStorage("./", nil), pks, Config{
		"0.0.0.0",
		8099,
	})
}