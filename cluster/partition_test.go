package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPartitionCollection_Get(t *testing.T) {
	collection := NewPartitionCollection()
	collection.Put(newPartition(2))

	assert.Equal(t, 2, collection.Get(3).Token)
	assert.Equal(t, 2, collection.Get(1).Token)
}

func TestPartitionCollection_GetMultiple(t *testing.T) {
	collection := NewPartitionCollection()
	collection.Put(newPartition(2))
	collection.Put(newPartition(5))
	collection.Put(newPartition(9))

	res := collection.GetMultiple(3, 3)
	assert.Equal(t, 2, res[0].Token)
	assert.Equal(t, 5, res[1].Token)
	assert.Equal(t, 9, res[2].Token)

	res2 := collection.GetMultiple(3, 4)
	assert.Equal(t, 3, len(res2))
	assert.Equal(t, 2, res2[0].Token)
	assert.Equal(t, 5, res2[1].Token)
	assert.Equal(t, 9, res2[2].Token)

	res3 := collection.GetMultiple(10, 2)
	assert.Equal(t, 9, res3[0].Token)
	assert.Equal(t, 2, res3[1].Token)
}

func newPartition(token int) *Partition {
	return &Partition{token, &Node{}}
}