package cluster

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResolver_FindAll(t *testing.T) {
	resolver := NewResolver()
	assert.Len(t, resolver.FindAll(), 0)

	resolver.AddToken(1, &Node{})
	resolver.AddToken(2, &Node{})

	assert.Len(t, resolver.FindAll(), 2)
}

func TestResolver_FindByKey(t *testing.T) {
	resolver := NewResolver()
	assert.Len(t, resolver.FindByKey(2, READ), 0)

	resolver.AddToken(1, &Node{[]int{1}, STATUS_UP, ":8086", "local"})
	resolver.AddToken(3, &Node{})

	locations := resolver.FindByKey(2, READ)
	assert.Len(t, locations, 2)
	assert.Contains(t, locations, ":8086")

	resolver.RemoveToken(1)

	locations2 := resolver.FindByKey(2, READ)
	assert.Contains(t, locations2, "")
}
