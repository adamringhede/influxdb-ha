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
	Save(node *Node) error
	// Remove removes a node by name
	Remove(name string) (bool, error)
	// OnRemove is passed a handler to be called when a node is removed.
	OnRemove(func(Node))
}

type EtcdNodeStorage struct {
	EtcdStorageBase
	onRemoveHandler func(Node)
}

func NewEtcdNodeStorage(c *clientv3.Client) *EtcdNodeStorage {
	s := &EtcdNodeStorage{}
	s.Client = c
	return s
}

func (s *EtcdNodeStorage) Watch() clientv3.WatchChan {
	return s.Client.Watch(context.Background(), s.path("nodes"), clientv3.WithPrefix())
}

func (s *EtcdNodeStorage) Save(node *Node) error {
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	s.Client.Put(context.Background(), s.path("nodes/"+node.Name), string(data))
	return nil
}

func (s *EtcdNodeStorage) Get(name string) (*Node, error) {
	resp, getErr := s.Client.Get(context.Background(), s.path("nodes/"+name))
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
	resp, getErr := s.Client.Get(context.Background(), s.path("nodes"), clientv3.WithPrefix())
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

func (s *EtcdNodeStorage) OnRemove(h func(Node)) {
	if s.onRemoveHandler != nil {
		panic("OnRemove is already defined")
	}
	s.onRemoveHandler = h
}

func (s *EtcdNodeStorage) Remove(name string) (bool, error) {
	node, err := s.Get(name)
	if node == nil {
		return false, nil
	}
	resp, err := s.Client.Delete(context.Background(), s.path("nodes/"+name))
	if err != nil {
		return false, err
	}
	if resp.Deleted > 0 {
		// TODO Handle failure here. If this fails, then the data may be lost.
		if s.onRemoveHandler != nil {
			s.onRemoveHandler(*node)
		}
		return true, nil
	}
	return false, nil
}

func (s *EtcdNodeStorage) RemoveAll(name string) (int, error) {
	resp, err := s.Client.Delete(context.Background(), s.path("nodes/"), clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return int(resp.Deleted), nil
}
