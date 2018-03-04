package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

func newEtcdStorage() *EtcdTokenStorage {
	storage := NewEtcdTokenStorage()
	storage.Open([]string{"http://127.0.0.1:2379"})
	storage.Clear()
	return storage
}

func TestEtcdTokenStorage_Watch(t *testing.T) {
	storage := newEtcdStorage()
	ch := storage.Watch()
	storage.Assign(1, "foo")
	result := <-ch
	assert.Len(t, result.Events, 1)
	event := result.Events[0]
	assert.Equal(t, mvccpb.PUT, event.Type)
	assert.Equal(t, "foo", string(event.Kv.Value))
}

func TestEtcdTokenStorage_SuggestReservations(t *testing.T) {
	storage := newEtcdStorage()
	_, err := storage.InitMany("bar", 16)
	assert.NoError(t, err)
	suggestions, err := storage.SuggestReservations()
	assert.NoError(t, err)
	assert.Len(t, suggestions, 8)
}

func TestEtcdTokenStorage_Init(t *testing.T) {
	storage := newEtcdStorage()
	_, err := storage.InitMany("bar", 4)
	assert.NoError(t, err)
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