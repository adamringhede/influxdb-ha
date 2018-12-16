package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolver_FindAll(t *testing.T) {
	resolver := NewResolver()
	assert.Len(t, resolver.FindAll(), 0)

	resolver.AddToken(1, &Node{Name: "a", DataLocation: "a"})
	resolver.AddToken(2, &Node{Name: "b", DataLocation: "b"})

	assert.Len(t, resolver.FindAll(), 2)
}

func TestResolver_FindByKey(t *testing.T) {
	resolver := NewResolver()
	resolver.ReplicationFactor = 2
	assert.Len(t, resolver.FindByKey(2, READ), 0)

	resolver.AddToken(1, &Node{[]int{1}, NodeStatusUp, ":8086", "local"})
	resolver.AddToken(3, &Node{[]int{3}, NodeStatusJoining, ":9096", "local2"})

	locations := resolver.FindByKey(1, READ)
	assert.Len(t, locations, 1)
	assert.Contains(t, locations, ":8086")

	resolver.RemoveToken(1)

	locations2 := resolver.FindByKey(2, WRITE)
	assert.Len(t, locations2, 1)
	assert.Contains(t, locations2, ":9096")
}

func TestResolver_ReverseSecondaryLookup(t *testing.T) {
	resolver := NewResolver()
	node1 := &Node{[]int{}, NodeStatusUp, ":8081", "local"}
	node2 := &Node{[]int{}, NodeStatusUp, ":8082", "local2"}

	resolver.AddToken(1, node1)
	resolver.AddToken(2, node2)
	resolver.AddToken(3, node1)
	resolver.AddToken(4, node1)
	resolver.AddToken(5, node1)
	resolver.AddToken(6, node2)

	assert.Equal(t, []int{6}, resolver.ReverseSecondaryLookup(1))
	assert.Equal(t, []int{1}, resolver.ReverseSecondaryLookup(2))
	assert.Equal(t, []int{2}, resolver.ReverseSecondaryLookup(3))

	// Due to the non-uniform layout of the tokens, these are never the target for replication
	// with a replication factor of 2 as all data in node 2 is replicated to token 3 and 6
	assert.Empty(t, resolver.ReverseSecondaryLookup(4))
	assert.Empty(t, resolver.ReverseSecondaryLookup(5))

	// This illustrates that an uneven token distribution leads to some tokens to carry more data than others
	assert.Equal(t, []int{3, 4, 5}, resolver.ReverseSecondaryLookup(6))
}
