package cluster

import (
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
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
	GetByTarget(target string) (map[string]HintStatus, error)
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

// TODO If the etcd cluster is not available, it needs to retry over and over again
// until it works or the node will not be aware that the data exist.
func (s *EtcdHintStorage) Put(target string, status HintStatus) error {
	if !s.Local[target] {
		s.Local[target] = true
		_, err := s.Client.Put(context.Background(), path.Join(s.path(etcdStorageHints), target, s.Holder), strconv.Itoa(int(status)))
		return err
	}
	return nil
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

func (s *EtcdHintStorage) GetByTarget(target string) (map[string]HintStatus, error) {
	resp, err := s.Client.Get(context.Background(), path.Join(s.path(etcdStorageHints), target), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	holderMap := map[string]HintStatus{}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		holder := parts[len(parts)-1]
		status, err := strconv.Atoi(string(kv.Value))
		if err == nil {
			holderMap[holder] = HintStatus(status)
		}
	}
	return holderMap, nil
}

// WaitUntilRecovered is used to block until no more node has data that should be recovered.
func WaitUntilRecovered(storage *EtcdHintStorage, nodeName string) chan struct{} {
	hints, _ := storage.GetByTarget(nodeName)
	doneCh := make(chan struct{}, 1)
watch:
	for update := range storage.Watch() {
		for _, event := range update.Events {
			parts := strings.Split(string(event.Kv.Key), "/")
			holder := parts[len(parts)-1]
			target := parts[len(parts)-2]
			if event.Type == mvccpb.DELETE && target == nodeName {
				delete(hints, holder)
				if len(hints) == 0 {
					doneCh <- struct{}{}
					break watch
				}
			}
		}
	}
	return doneCh
}

// WaitUntilRecovered is used to block until no more node has data that should be recovered.
func WhenRecovered(storage *EtcdHintStorage, nodeName string, cb func()) {
	go func() {
		<-WaitUntilRecovered(storage, nodeName)
		cb()
	}()
}
