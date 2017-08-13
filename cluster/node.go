package cluster

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
	"os"
	"log"
	"strconv"
)

const (
	NodeStatusRemoved    = iota
	NodeStatusUp
	NodeStatusJoining
	NodeStatusIdle
	NodeStatusRecovering
)

type Node struct {
	Tokens       []int
	Status       int
	DataLocation string
	Name         string
}

type nodeMeta struct {
	Status       int
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
	node.Status = NodeStatusIdle
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
			strconv.FormatInt(int64(data.Tokens[0]),10))
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
