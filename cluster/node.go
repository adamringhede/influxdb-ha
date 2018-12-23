package cluster

import (
	"encoding/json"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

type NodeStatus int

const (
	NodeStatusRemoved NodeStatus = iota
	NodeStatusUp
	NodeStatusJoining
	NodeStatusStarting
	NodeStatusRecovering
)

type Node struct {
	Tokens       []int
	Status       NodeStatus
	DataLocation string
	Name         string
}

func (node *Node) String() string {
	return node.Name
}

// NodeCollection holds objects which represent nodes in the cluster. It's methods are supposed to be
// fast to support efficient lookups. Node objects are passed as values to prevent changing the state across
// different parts of the program. The NodeCollection is supposed to represent the true state of the cluster's nodes.
type NodeCollection interface {
	Get(name string) (Node, bool)
	GetAll() map[string]Node
	Len() int
	Persist(node Node) error
}

type LocalNodeCollection struct {
	nodes map[string]*Node
}

func NewLocalNodeCollection() *LocalNodeCollection {
	return &LocalNodeCollection{make(map[string]*Node)}
}

func (c *LocalNodeCollection) Len() int {
	return len(c.nodes)
}

func (c *LocalNodeCollection) Get(name string) (Node, bool) {
	if node, ok := c.nodes[name]; ok {
		return *node, true
	}
	return Node{}, false
}

func (c *LocalNodeCollection) GetAll() map[string]Node {
	res := make(map[string]Node, len(c.nodes))
	for name, node := range c.nodes {
		res[name] = *node
	}
	return res
}

func (c *LocalNodeCollection) Persist(node Node) error {
	c.nodes[node.Name] = &node
	return nil
}

// SyncedNodeCollection is backed by a persistent etcd storage to keep itself up to date.
type SyncedNodeCollection struct {
	LocalNodeCollection
	storage *EtcdNodeStorage
	closeCh chan bool
}

func (c *SyncedNodeCollection) Persist(node Node) error {
	err := c.storage.Save(&node)
	if err != nil {
		return err
	}
	c.LocalNodeCollection.Persist(node)
	return nil
}

func (c *SyncedNodeCollection) Get(name string) (Node, bool) {
	node, ok := c.LocalNodeCollection.Get(name)
	if !ok {
		foundNode, err := c.storage.Get(name)
		if err != nil {
			node = *foundNode
			ok = true
			c.LocalNodeCollection.Persist(node)
		}
	}
	return node, ok
}

func (c *SyncedNodeCollection) trackUpdates() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case update := <-c.storage.Watch():
			for _, event := range update.Events {
				if event.Type == mvccpb.PUT {
					var node Node
					err := json.Unmarshal(event.Kv.Value, &node)
					if err != nil {
						panic("Failed to parse node from update: " + string(event.Kv.Value))
					}
					if _, ok := c.nodes[node.Name]; ok {
						c.nodes[node.Name].Status = node.Status
						c.nodes[node.Name].DataLocation = node.DataLocation
					} else {
						c.nodes[node.Name] = &node
					}
				}
			}
		case <-ticker.C:
			c.updateFromStorage()
		case <-c.closeCh:
			return
		}
	}
}

func (c *SyncedNodeCollection) updateFromStorage() error {
	nodes, err := c.storage.GetAll()
	if err != nil {
		return err
	}
	nodesMap := make(map[string]*Node, len(nodes))
	for _, node := range nodes {
		nodesMap[node.Name] = node
	}
	c.nodes = nodesMap
	return nil
}

// NewSyncedNodeCollection fetches existing node from the etcd node storage and watches for changes
func NewSyncedNodeCollection(storage *EtcdNodeStorage) (*SyncedNodeCollection, error) {
	c := &SyncedNodeCollection{storage: storage, closeCh: make(chan bool)}
	c.nodes = map[string]*Node{}
	err := c.updateFromStorage()
	if err != nil {
		return nil, err
	}
	go c.trackUpdates()
	return c, nil
}

// Close stops watching for events on update events from the node storage.
// Note that the node storage's etcd client need to be stopped as well.
func (c *SyncedNodeCollection) Close() {
	close(c.closeCh)
}
