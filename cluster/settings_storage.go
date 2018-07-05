package cluster

import (
	"github.com/coreos/etcd/clientv3"
	"encoding/json"
	"context"
	"strconv"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"strings"
)

const etcdStorageSettings = "settings"


type EtcdSettingsStorage struct {
	EtcdStorageBase
}

func NewEtcdSettingsStorage(c *clientv3.Client) *EtcdSettingsStorage {
	s := &EtcdSettingsStorage{}
	s.Client = c
	return s
}

func (s *EtcdSettingsStorage) Watch() clientv3.WatchChan {
	return s.Client.Watch(context.Background(), s.path(etcdStorageSettings))
}

func (s *EtcdSettingsStorage) Get(partitionKey *PartitionKey) error {
	data, err := json.Marshal(partitionKey)
	if err != nil {
		return err
	}
	s.Client.Put(context.Background(), s.path(etcdStorageSettings) + partitionKey.Identifier(), string(data))
	return nil
}

func (s *EtcdSettingsStorage) set(key, value string) error {
	_, err := s.Client.Put(context.Background(), s.path(etcdStorageSettings) + key, value)
	return err
}


func (s* EtcdSettingsStorage) watchKey(key string) chan string {
	updates := make(chan string)
	for update := range s.Watch() {
		for _, event := range update.Events {
			if event.Type == mvccpb.PUT {
				key := string(event.Kv.Key)
				if strings.HasPrefix(key, s.path(etcdStorageSettings) + key) {
					updates <- string(event.Kv.Value)
				}
			}
		}
	}
	return updates
}

func (s *EtcdSettingsStorage) GetDefaultReplicationFactor(fallback int) (int, error) {
	resp, err := s.Client.Get(context.Background(), s.path(etcdStorageSettings) + "rf_default")
	if err != nil {
		return fallback, err
	}
	if resp.Count == 0 {
		return fallback, nil
	}
	return strconv.Atoi(string(resp.Kvs[0].Value))
}


func (s* EtcdSettingsStorage) WatchDefaultReplicationFactor() chan int {
	updates := make(chan int)
	for update := range s.watchKey("rf_default") {
		value, _ := strconv.Atoi(update)
		updates <- value
	}
	return updates
}

func (s *EtcdSettingsStorage) SetDefaultReplicationFactor(factor int) error {
	return s.set("rf_default", strconv.Itoa(factor))
}


func (s *EtcdSettingsStorage) SetReplicationFactor(db, measurement string, factor int) error {
	return s.set("rf/" + db + "." + measurement, strconv.Itoa(factor))
}

func (s *EtcdSettingsStorage) GetAll() ([]*PartitionKey, error) {
	resp, getErr := s.Client.Get(context.Background(), s.path(etcdStorageSettings), clientv3.WithPrefix())
	if getErr != nil {
		return nil, getErr
	}
	var items []*PartitionKey
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