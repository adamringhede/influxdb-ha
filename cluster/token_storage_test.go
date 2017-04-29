package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func newEtcdStorage() *etcdTokenStorage {
	storage := &etcdTokenStorage{}
	storage.Open([]string{"http://127.0.0.1:2379"})
	return storage
}

func TestEtcdTokenStorage_Reserve(t *testing.T) {
	storage := newEtcdStorage()
	storage.Release(5)
	ok, err := storage.Reserve(5, "foo")
	assert.NoError(t, err)
	assert.True(t, ok)
	ok2, _ := storage.Reserve(5, "foo")
	assert.True(t, ok2)
	ok3, _ := storage.Reserve(5, "bar")
	assert.False(t, ok3)
}

func TestEtcdTokenStorage_Release(t *testing.T) {
	storage := newEtcdStorage()
	storage.Reserve(5, "foo")
	err := storage.Release(5)
	assert.NoError(t, err)
	ok, _ := storage.Reserve(5, "bar")
	assert.True(t, ok)
}

func TestEtcdTokenStorage_Assign(t *testing.T) {
	storage := newEtcdStorage()
	err := storage.Assign(3, "foo")
	assert.NoError(t, err)
	tokens, getErr := storage.Get()
	assert.NoError(t, getErr)
	assert.Contains(t, tokens, 3)
}