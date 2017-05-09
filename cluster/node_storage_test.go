package cluster

import (
	"testing"
	"github.com/coreos/etcd/clientv3"
	"time"
	"github.com/stretchr/testify/assert"
)

func createEtcdNodeStorage() *EtcdNodeStorage {
	c, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	return NewEtcdNodeStorage(c)
}

func TestEtcdNodeStorage_Save(t *testing.T) {
	storage := createEtcdNodeStorage()
	storage.Remove("a")

	node := &Node{}
	node.Name = "a"
	err := storage.Save(node)
	assert.NoError(t, err)

	savedNode, err := storage.Get("a")
	assert.NoError(t, err)
	assert.NotNil(t, savedNode)

	all, err := storage.GetAll()
	assert.NoError(t, err)
	assert.Len(t, all, 1)
}