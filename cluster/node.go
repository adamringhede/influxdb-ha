package cluster

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
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
	m := &meta{}
	err := gob.NewDecoder(buf).Decode(m)
	if err != nil {
		return err
	}
	node.Tokens = m.Tokens
	node.Status = m.Status
	node.DataLocation = m.DataLocation
	return nil
}

type LocalNode struct {
	Node
	storage localStorage
}

func NewLocalNode() *LocalNode {
	node := &LocalNode{}
	node.Status = STATUS_IDLE
	return node
}

func CreateNodeWithStorage(storage localStorage) *LocalNode {
	node := NewLocalNode()
	node.storage = storage
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
	return errors.New("Not implemented")
}

type localStorage interface {
	get() (persistentState, error)
	save(state persistentState) error
}

type persistentState struct {
	Tokens []int
}

type meta struct {
	Tokens       []int
	Status       int
	DataLocation string
}
