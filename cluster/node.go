package cluster

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/coreos/etcd/mvcc/mvccpb"
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

type nodeMeta struct {
	Status       NodeStatus
	DataLocation string
}

func (node *Node) updateFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	var m nodeMeta
	err := gob.NewDecoder(buf).Decode(&m)
	if err != nil {
		return err
	}
	// Tokens can not be sent as meta data as the amount of bytes would
	// easily exceed the hard limit of 512 as at this time 256 tokens are cerated
	// with 32 bits each. We could of course decrease this amount if needed, however
	// sending the tokens more than once is unnecessary and meta data should
	// be very limited to allow for more efficient state propagation.
	//node.Tokens = m.Tokens
	node.Status = m.Status
	node.DataLocation = m.DataLocation
	log.Printf("[Cluster] Info: Received node status %d from %s", m.Status, node.Name)
	log.Printf("I should send requests to %s", node.DataLocation)
	return nil
}

func (node *Node) String() string {
	return node.Name
}

type ClusterMeta struct {
	PartitionKeys []PartitionKey
}

type LocalNode struct {
	Node
	Meta    ClusterMeta
	storage localStorage
}

func NewLocalNode() *LocalNode {
	node := &LocalNode{}
	node.Status = NodeStatusStarting
	node.Meta = ClusterMeta{}
	return node
}

func CreateNodeWithStorage(storage localStorage) *LocalNode {
	node := NewLocalNode()
	node.storage = storage
	hostname, nameErr := os.Hostname()
	if nameErr != nil {
		panic(nameErr)
	}
	node.Name = hostname
	return node
}

// Init creates new tokens with random position on the
// ring given current all existing partitions to avoid conflicts and a more
// uniform size.
func (node *LocalNode) Init() error {
	data, err := node.storage.get()
	if err != nil {
		return err
	}
	if len(data.Tokens) == 0 {
		log.Println("[Cluster] Creating new tokens.")
		data.Tokens = generateTokens(32)
		node.storage.save(data)
	} else {
		log.Printf("[Cluster] Initializing with stored tokens. Starting with %s",
			strconv.FormatInt(int64(data.Tokens[0]), 10))
	}
	node.Tokens = data.Tokens
	node.Meta.PartitionKeys = data.PartitionKeys
	node.Status = NodeStatusUp
	return nil
}

func generateTokens(count int) []int {
	tokens := []int{}
	for i := 0; i < count; i++ {
		tokens = append(tokens, int(rand.Int31()))
	}
	return tokens
}

// Join looks up local configuration and uses it to determine
// if it should initiate with new tokens.
func (node *LocalNode) Join() error {
	return errors.New("Not implemented")
}

// Save stores its state in a local database.
func (node *LocalNode) Save() error {
	state := persistentState{
		node.Tokens,
		node.Meta.PartitionKeys,
	}
	return node.storage.save(state)
}

type localStorage interface {
	get() (persistentState, error)
	save(state persistentState) error
}

type persistentState struct {
	Tokens        []int
	PartitionKeys []PartitionKey
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
