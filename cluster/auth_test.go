package cluster

import (
	"github.com/coreos/etcd/clientv3"
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
)

func createEtcdAuthStorage() *EtcdAuthStorage {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	storage := NewEtcdAuthStorage(c)
	storage.ClusterID = "test-cluster"
	return storage
}

func TestEtcdAuthStorage(t *testing.T) {
	storage := createEtcdAuthStorage()
	storage.Delete()

	auth := NewAuthData()
	auth.Users = append(auth.Users, UserInfo{
		Name:  "tester",
		Hash:  HashUserPassword("password"),
		Admin: true,
	})

	watch := storage.Watch()

	err := storage.Save(auth)
	assert.NoError(t, err)

	auth, err = storage.Get()
	assert.NoError(t, err)
	assert.Len(t, auth.Users, 1)

	update := <-watch
	assert.Len(t, update.Users, 1)
}
