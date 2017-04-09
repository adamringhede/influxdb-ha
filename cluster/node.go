package cluster

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
	"os"
)

const (
	STATUS_UP = iota
	STATUS_JOINING
	STATUS_REMOVED
	STATUS_IDLE
)

type Node struct {
	Tokens       []int
	Status       int
	DataLocation string
	Name         string
}

func (node *Node) updateFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)
	var m struct {
		Tokens       []int
		Status       int
		DataLocation string
	}
	err := gob.NewDecoder(buf).Decode(&m)
	if err != nil {
		return err
	}
	node.Tokens = m.Tokens
	node.Status = m.Status
	node.DataLocation = m.DataLocation
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
	node.Status = STATUS_IDLE
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
		data.Tokens = generateTokens(256)
		node.storage.save(data)
	}
	node.Tokens = data.Tokens
	node.Meta.PartitionKeys = data.PartitionKeys
	node.Status = STATUS_UP
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
