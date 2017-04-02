package cluster

import (
	"errors"
	"encoding/gob"
	"bytes"
)

const (
	STATUS_UP = iota
	STATUS_JOINING
	STATUS_REMOVED
	STATUS_IDLE
)

type Node struct {
	Tokens		[]int
	Status		int
	DataLocation	string
	Name 		string
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
}

func NewLocalNode() *LocalNode {
	node := &LocalNode{}
	node.Status = STATUS_IDLE
	return node
}

// Init creates new tokens with random position on the
// ring given current all existing partitions to avoid conflicts and a more
// uniform size.
func (node *LocalNode) Init() error {
	return errors.New("Not implemented")
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

type meta struct {
	Tokens		[]int
	Status		int
	DataLocation	string
}