package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPartitionCollection_Get(t *testing.T) {
	collection := NewPartitionCollection()
	collection.Put(newPartition(1))

	assert.Equal(t, 1, collection.Get(2).Token)
}

func newPartition(token int) *Partition {
	return &Partition{1, &Node{}}
}