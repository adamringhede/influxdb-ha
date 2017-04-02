package cluster

import (
	"github.com/emirpasic/gods/trees/redblacktree"
)

type Partition struct {
	Token	int
	Node	*Node
}

// PartitionCollection is for partition look ups
// backed by a red black tree.
type PartitionCollection struct {
	tree	*redblacktree.Tree
}

func NewPartitionCollection() *PartitionCollection {
	return &PartitionCollection{redblacktree.NewWithIntComparator()}
}

func (c *PartitionCollection) Put(partition *Partition) {
	c.tree.Put(partition.Token, partition)
}

func (c *PartitionCollection) Remove(key int) {
	c.tree.Remove(key)
}

func (c *PartitionCollection) Get(key int) *Partition {
	iter, ok := c.findNode(key)
	if ok {
		return iter.Value().(*Partition)
	}
	return nil
}

func (c *PartitionCollection) GetMultiple(key int, count int) []*Partition {
	res := []*Partition{}
	found := make(map[int]bool)
	iter, ok := c.findNode(key)
	if !ok {
		return res
	}
	for len(res) <= count {
		k := iter.Key().(int)
		if _, ok := found[k]; ok {
			res = append(res, iter.Value().(*Partition))
			found[k] = true
		} else {
			break
		}
		if !iter.Next() {
			iter.First()
		}
	}
	return res
}

func (c *PartitionCollection) findNode(key int) (*redblacktree.Iterator, bool) {
	if c.tree.Size() == 0 {
		return nil, false
	}
	current := c.tree.Root
	for {
		switch c.tree.Comparator(key, current.Key) {
		case 1:
			if current.Right == nil {
				return &redblacktree.Iterator{
					c.tree,
					current,
					1,
				}, true
			}
			current = current.Right
		case -1:
			if current.Left == nil {
				iter := &redblacktree.Iterator{
					c.tree,
					current,
					1,
				}
				if !iter.Prev() {
					iter.Last()
				}
				return iter, true

			}
			current = current.Left
		default:
			return &redblacktree.Iterator{
				c.tree,
				current,
				1,
			}, true
		}
	}
}
