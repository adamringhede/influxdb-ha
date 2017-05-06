package cluster

import (
	"fmt"
	"errors"
	"strings"
	"github.com/adamringhede/influxdb-ha/hash"
)

// Partitioner is used to generate numerical hash values based on a given
// set of values and a set of partition keys. The partition keys can be
// changed by an administrator while the server is running.
type Partitioner struct {
	partitionKeys map[string]PartitionKey
}

func NewPartitioner() *Partitioner {
	return &Partitioner{make(map[string]PartitionKey)}
}

func (p *Partitioner) GetHash(key PartitionKey, values map[string][]string) (int, error) {
	compoundKey := []string{}
	for _, tag := range key.Tags {
		if values, ok := values[tag]; ok {
			// if there are multiple values, multiple keys have to be created.
			if len(values) > 1 {
				return 0, errors.New("Multiple keys are not yet supported")
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
func (p *Partitioner) FulfillsKey(key PartitionKey, values map[string][]string) bool {
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

func (p *Partitioner) AddKey(key PartitionKey) {
	p.partitionKeys[key.Database+ "." + key.Measurement] = key
}

func (p *Partitioner) RemoveKey(key PartitionKey) {
	delete(p.partitionKeys, key.Database+ "." + key.Measurement)
}

func (p *Partitioner) GetKeyByMeasurement(db string, msmt string) (PartitionKey, bool) {
	key, ok := p.partitionKeys[db + "." + msmt]
	return key, ok
}

func (p *Partitioner) AddKeys(keys []PartitionKey) {
	for _, k := range keys {
		p.AddKey(k)
	}
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
