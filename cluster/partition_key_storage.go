package cluster

import (
	"github.com/coreos/etcd/clientv3"
	"context"
	"encoding/json"
)

const etcdStoragePartitionKeys = "partitionKeys"

type PartitionKeyStorage interface {
	Watch() clientv3.WatchChan
	Drop(db, msmt string) error
	Save(*PartitionKey) error
	GetAll() ([]*PartitionKey, error)
}

type EtcdPartitionKeyStorage struct {
	EtcdStorageBase
}

func NewEtcdPartitionKeyStorage(c *clientv3.Client) *EtcdPartitionKeyStorage {
	s := &EtcdPartitionKeyStorage{}
	s.Client = c
	return s
}

func (s *EtcdPartitionKeyStorage) Watch() clientv3.WatchChan {
	return s.Client.Watch(context.Background(), s.path(etcdStoragePartitionKeys), clientv3.WithPrefix())
}

func (s *EtcdPartitionKeyStorage) Save(partitionKey *PartitionKey) error {
	data, err := json.Marshal(partitionKey)
	if err != nil {
		return err
	}
	_, err = s.Client.Put(context.Background(), s.path(etcdStoragePartitionKeys) + partitionKey.Identifier(), string(data))
	return err
}

func (s *EtcdPartitionKeyStorage) Drop(db, msmt string) error {
	id := CreatePartitionKeyIdentifier(db, msmt)
	_, err := s.Client.Delete(context.Background(), s.path(etcdStoragePartitionKeys) + id)
	return err
}

func (s *EtcdPartitionKeyStorage) GetAll() ([]*PartitionKey, error) {
	resp, getErr := s.Client.Get(context.Background(), s.path(etcdStoragePartitionKeys), clientv3.WithPrefix())
	if getErr != nil {
		return nil, getErr
	}
	items := []*PartitionKey{}
	for _, kv := range resp.Kvs {
		var node PartitionKey
		valueErr := json.Unmarshal(kv.Value, &node)
		if valueErr != nil {
			return nil, valueErr
		}
		items = append(items, &node)
	}
	return items, nil
}