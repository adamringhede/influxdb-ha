package cluster

import (
	"github.com/emirpasic/gods/trees/avltree"
)

type Partition struct {
	Token int
	Node  *Node
}

// PartitionCollection is for partition look ups.
// Partitions are on a ring and "get" methods returns a partition
// given a token on that ring. When finding multiple partitions
// for a single token, multiple partitions are never returned for
// belonging to the same node as that would undermine the replication factor.
type PartitionCollection struct {
	tree *avltree.Tree
}

func NewPartitionCollection() *PartitionCollection {
	return &PartitionCollection{avltree.NewWithIntComparator()}
}

func (c *PartitionCollection) Size() int {
	return c.tree.Size()
}

func (c *PartitionCollection) Put(partition *Partition) {
	c.tree.Put(partition.Token, partition)
}

func (c *PartitionCollection) Remove(key int) {
	c.tree.Remove(key)
}

func (c *PartitionCollection) Clear() {
	c.tree.Clear()
}

func (c *PartitionCollection) Get(key int) *Partition {
	node, ok := c.findNode(key)
	if !ok {
		return nil
	}
	return node.Value.(*Partition)
}

// GetMultiple returns a specified count of partitions with unique nodes.
func (c *PartitionCollection) GetMultiple(key int, count int) []*Partition {
	res := []*Partition{}
	node, ok := c.findNode(key)
	if !ok {
		return res
	}
	// found is used to know that we should stop searching
	found := make(map[int]bool, count)
	// unique is to make sure we don't get the same node more than once
	unique := make(map[string]bool, count)
	for len(res) < count {
		k := node.Key.(int)
		if _, ok := found[k]; !ok {
			partition := node.Value.(*Partition)
			if _, alreadyAdded := unique[partition.Node.Name]; !alreadyAdded {
				res = append(res, partition)
				found[k] = true
				unique[partition.Node.Name] = true
			}
		} else {
			break
		}
		node = node.Next()
		if node == nil {
			node = c.tree.Left()
		}
	}
	return res
}

func (c *PartitionCollection) GetReverse(key int, count int) []*Partition {
	res := []*Partition{}
	node, ok := c.findNode(key)
	if !ok {
		return res
	}
	// found is used to know that we should stop searching
	found := make(map[int]bool, count)
	// unique is to make sure we don't get the same node more than once
	unique := make(map[string]bool, count)
	for len(res) < count {
		node = node.Prev()
		if node == nil {
			node = c.tree.Right()
		}
		k := node.Key.(int)
		if _, ok := found[k]; !ok && k != key {
			partition := node.Value.(*Partition)
			//if _, alreadyAdded := unique[partition.Node.Name]; !alreadyAdded {
			res = append(res, partition)
			found[k] = true
			unique[partition.Node.Name] = true
			//}
		} else {
			break
		}
	}
	return res
}

func (c *PartitionCollection) findNode(key int) (*avltree.Node, bool) {
	if c.tree.Size() == 0 {
		return nil, false
	}
	node, ok := c.tree.Floor(key)
	if !ok {
		node = c.tree.Left()
	}
	return node, true
}
