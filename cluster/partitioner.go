package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/adamringhede/influxdb-ha/hash"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/schwarmco/go-cartesian-product"
)

const PartitionTagName = "_partitionToken"

// Partitioner is used to generate numerical hash values based on a given
// set of values and a set of partition keys. The partition keys can be
// changed by an administrator while the server is running.
type Partitioner interface {
	GetHashes(key PartitionKey, tags map[string][]string) []int
	// FulfillsKey checks if the given values are enough for the given partition key
	FulfillsKey(key PartitionKey, values map[string][]string) bool
	AddKey(key PartitionKey)
	RemoveKey(key PartitionKey)
	GetKeyByMeasurement(db string, msmt string) (PartitionKey, bool)
	AddKeys(keys []PartitionKey)
}

type PartitionKeyCollection interface {
	GetPartitionKeys() []PartitionKey
}

type BasicPartitioner struct {
	Partitioner
	partitionKeys map[string]PartitionKey
}

func NewPartitioner() *BasicPartitioner {
	return &BasicPartitioner{partitionKeys: make(map[string]PartitionKey)}
}

func (p *BasicPartitioner) GetPartitionKeys() []PartitionKey {
	keys := []PartitionKey{}
	for _, key := range p.partitionKeys {
		keys = append(keys, key)
	}
	return keys
}

func createCompoundKeys(key PartitionKey, tags map[string][]string) []string {
	partitionValues := [][]interface{}{}
	for _, tag := range key.Tags {
		if values, ok := tags[tag]; ok {
			s := make([]interface{}, len(values))
			for i, v := range values {
				s[i] = v
			}
			partitionValues = append(partitionValues, s)
		} else {
			return []string{}
		}
	}
	combinations := []string{}
	for tagsValues := range cartesian.Iter(partitionValues...) {
		combination := ""
		for _, value := range tagsValues {
			combination += value.(string)
		}
		combinations = append(combinations, combination)
	}
	return combinations
}

func (p *BasicPartitioner) GetHashes(key PartitionKey, tags map[string][]string) []int {
	hashes := []int{}
	for _, combination := range createCompoundKeys(key, tags) {
		hashes = append(hashes, int(hash.String(combination)))
	}
	return hashes
}

func GetHash(key PartitionKey, tags map[string][]string) (int, error) {
	compoundKey := []string{}
	for _, tag := range key.Tags {
		if values, ok := tags[tag]; ok {
			/*
				If there are multiple values, multiple hashes need to be returned for every possible combination.
				This requires that the query coordinator makes the request to multiple nodes and then merges the
				results.
			*/
			if len(values) > 1 {
				return 0, errors.New("Multiple tag values for the same tag is not supported")
			}
			compoundKey = append(compoundKey, values[0])
		} else {
			return 0, fmt.Errorf("The partition key for measurement %s requires tags [%s]",
				key.Measurement, strings.Join(key.Tags, ", "))
		}
	}
	numericHash := int(hash.String(strings.Join(compoundKey, "")))
	return numericHash, nil
}

// FulfillsKey checks if the given values are enough for the given partition key
func (p *BasicPartitioner) FulfillsKey(key PartitionKey, values map[string][]string) bool {
	for _, tag := range key.Tags {
		if v, ok := values[tag]; ok {
			if len(v) == 0 || v[0] == "" {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func (p *BasicPartitioner) AddKey(key PartitionKey) {
	p.partitionKeys[key.Identifier()] = key
}

func (p *BasicPartitioner) RemoveKey(key PartitionKey) {
	delete(p.partitionKeys, key.Identifier())
}

func (p *BasicPartitioner) GetKeyByMeasurement(db string, msmt string) (PartitionKey, bool) {
	key, ok := p.partitionKeys[db+"."+msmt]
	if !ok {
		key, ok = p.partitionKeys[db]
	}
	return key, ok
}

func (p *BasicPartitioner) AddKeys(keys []PartitionKey) {
	for _, k := range keys {
		p.AddKey(k)
	}
}

type SyncedPartitioner struct {
	BasicPartitioner
	storage PartitionKeyStorage
	closeCh chan bool
}

func (c *SyncedPartitioner) trackUpdates() {
	for {
		select {
		case update := <-c.storage.Watch():
			for _, event := range update.Events {
				var partitionKey PartitionKey
				err := json.Unmarshal(event.Kv.Value, &partitionKey)
				if err != nil {
					panic("Failed to parse partition key from update: " + string(event.Kv.Value))
				}
				if event.Type == mvccpb.PUT {
					c.AddKey(partitionKey)
				} else if event.Type == mvccpb.DELETE {
					// Removing a partition key requires that data is re-distributed which must happen
					// before the key is removed.
					c.RemoveKey(partitionKey)
				}
			}
		case <-c.closeCh:
			return
		}
	}
}

func (c *SyncedPartitioner) updateFromStorage() error {
	pks, err := c.storage.GetAll()
	if err != nil {
		return err
	}
	pksMap := make(map[string]PartitionKey, len(pks))
	for _, pk := range pks {
		pksMap[pk.Identifier()] = *pk
	}
	c.partitionKeys = pksMap
	return nil
}

func (c *SyncedPartitioner) Close() {
	close(c.closeCh)
}

// NewSyncedPartitioner fetches existing keys from storage
func NewSyncedPartitioner(storage PartitionKeyStorage) (*SyncedPartitioner, error) {
	c := &SyncedPartitioner{storage: storage, closeCh: make(chan bool)}
	err := c.updateFromStorage()
	if err != nil {
		return nil, err
	}
	go c.trackUpdates()
	return c, nil
}

type PartitionKey struct {
	// Database is the name of the database for which the partition key should be used.
	// It is a required field.
	Database string

	// Measurement is used to limit the partition key to a certain measurement
	// If left empty, the key will be used for all measurements in the specified database.
	Measurement string

	// Tags contains an ordered set of tags. All writes need to includes all tags
	// specified in the the key. Reads can only make use of it if all tags in the
	// key are also in the query.
	Tags []string
}

func (pk *PartitionKey) Identifier() string {
	return CreatePartitionKeyIdentifier(pk.Database, pk.Measurement)
}

func CreatePartitionKeyIdentifier(database, measurement string) string {
	if measurement == "" {
		return database
	}
	return database + "." + measurement
}
