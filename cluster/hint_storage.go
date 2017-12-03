package cluster

import (
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"
)

type HintStatus int

const (
	// StatusWaiting is used to signal that the node is
	StatusWaiting HintStatus = iota
	StatusRecovering
)

const etcdStorageHints = "hints"

// HintStorage should hold information about what nodes hold data for a certain target node.
type HintStorage interface {
	Put(target string, status HintStatus) error
	Done(target string) error
	// GetByTarget returns the nodes that currently holds data for the node and the status of recovery
	GetByTarget(target string) (map[string]int, error)
	GetByHolder() ([]string, error)
}

type EtcdHintStorage struct {
	EtcdStorageBase
	Holder string
	Local  map[string]bool
}

func NewEtcdHintStorage(c *clientv3.Client, holder string) *EtcdHintStorage {
	s := &EtcdHintStorage{}
	s.Client = c
	s.Holder = holder
	s.Local = map[string]bool{}
	localTargets, err := s.GetByHolder()
	if err != nil {
		for _, target := range localTargets {
			s.Local[target] = true
		}
	}
	return s
}

func (s *EtcdHintStorage) Watch() clientv3.WatchChan {
	return s.Client.Watch(context.Background(), s.path(etcdStorageHints), clientv3.WithPrefix())
}

func (s *EtcdHintStorage) Put(target string, status HintStatus) error {
	s.Local[target] = true
	_, err := s.Client.Put(context.Background(), path.Join(s.path(etcdStorageHints), target, s.Holder), strconv.Itoa(int(status)))
	return err
}

func (s *EtcdHintStorage) Done(target string) error {
	delete(s.Local, target)
	_, err := s.Client.Delete(context.Background(), path.Join(s.path(etcdStorageHints), target, s.Holder))
	return err
}

func (s *EtcdHintStorage) GetByHolder() ([]string, error) {
	resp, err := s.Client.Get(context.Background(), s.path(etcdStorageHints), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	targets := []string{}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		holder := parts[len(parts)-1]
		if holder == s.Holder {
			targets = append(targets, parts[len(parts)-2])
		}
	}
	return targets, nil
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
