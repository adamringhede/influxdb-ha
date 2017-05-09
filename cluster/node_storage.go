package cluster

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
)

type NodeStorage interface {
	// GetAll returns all nodes
	GetAll() ([]*Node, error)
	// Get returns the node by name or nil if not found
	Get(string) (*Node, error)
	// Save saves the node. It will overwrite existing data or insert the node.
	Save(node Node)
}

type EtcdNodeStorage struct {
	etcdStorageBase
}

func NewEtcdNodeStorage(c *clientv3.Client) *EtcdNodeStorage {
	s := &EtcdNodeStorage{}
	s.client = c
	return s
}

func (s *EtcdNodeStorage) Watch() clientv3.WatchChan {
	return s.client.Watch(context.Background(), s.path("nodes"), clientv3.WithPrefix())
}

func (s *EtcdNodeStorage) Save(node *Node) error {
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	s.client.Put(context.Background(), s.path("nodes/" + node.Name), string(data))
	return nil
}

func (s *EtcdNodeStorage) Get(name string) (*Node, error) {
	resp, getErr := s.client.Get(context.Background(), s.path("nodes/" + name))
	if getErr != nil || resp.Count == 0 {
		return nil, getErr
	}
	var node Node
	valueErr := json.Unmarshal(resp.Kvs[0].Value, &node)
	if valueErr != nil {
		return nil, valueErr
	}
	return &node, nil
}

func (s *EtcdNodeStorage) GetAll() ([]*Node, error) {
	resp, getErr := s.client.Get(context.Background(), s.path("nodes"), clientv3.WithPrefix())
	if getErr != nil {
		return nil, getErr
	}
	nodes := []*Node{}
	for _, kv := range resp.Kvs {
		var node Node
		valueErr := json.Unmarshal(kv.Value, &node)
		if valueErr != nil {
			return nil, valueErr
		}
		nodes = append(nodes, &node)
	}
	return nodes, nil
}

func (s *EtcdNodeStorage) Remove(name string) (bool, error) {
	resp, err := s.client.Delete(context.Background(), s.path("nodes/" + name))
	if err != nil {
		return false, err
	}
	return resp.Deleted > 0, nil
}

func (s *EtcdNodeStorage) RemoveAll(name string) (int, error) {
	resp, err := s.client.Delete(context.Background(), s.path("nodes/"), clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return int(resp.Deleted), nil
}
