package cluster

import (
	"github.com/coreos/etcd/clientv3"
	"strconv"
	"context"
	"path"
	"strings"
)

const (
	StatusWaiting = iota
	StatusRecovering
)

const etcdStorageHints = "hints"

// HintStorage should hold information about what nodes hold data for a certain target node.
type HintStorage interface {
	Put(target string, status int) error
	// GetByTarget returns the nodes that currently holds data for the node and the status of recovery
	GetByTarget(target string) (map[string]int, error)
}

type EtcdHintStorage struct {
	EtcdStorageBase
	// Holder is the node name
	Holder string
}

func NewEtcdHintStorage(c *clientv3.Client, holder string) *EtcdHintStorage {
	s := &EtcdHintStorage{}
	s.Client = c
	s.Holder = holder
	return s
}

func (s *EtcdHintStorage) Watch() clientv3.WatchChan {
	return s.Client.Watch(context.Background(), s.path(etcdStorageHints), clientv3.WithPrefix())
}

func (s *EtcdHintStorage) Put(target string, status int) error {
	_, err := s.Client.Put(context.Background(), path.Join(s.path(etcdStorageHints), target, s.Holder), strconv.Itoa(status))
	return err
}

func (s *EtcdHintStorage) GetByTarget(target string) (map[string]int, error) {
	resp, err := s.Client.Get(context.Background(), path.Join(s.path(etcdStorageHints), target), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	holderMap := map[string]int{}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		holder := parts[len(parts)-1]
		status, err := strconv.Atoi(string(kv.Value))
		if err == nil {
			holderMap[holder] = status
		}
	}
	return holderMap, nil
}