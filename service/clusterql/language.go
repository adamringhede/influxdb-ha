package clusterql

import "strings"

func CreateLanguage() *Language {
	lang := NewLanguage()
	lang.Spec(SHOW, PARTITION, KEYS).Handle(func (params Params) Statement {
		return ShowPartitionKeysStatement{}
	})
	lang.Spec(SHOW, PARTITION, KEYS, ON, STR).Handle(func (params Params) Statement {
		return ShowPartitionKeysStatement{Database: params[0]}
	})
	// TODO Consider having/requiring database as a default parameter
	// TODO Add validation
	lang.Spec(CREATE, PARTITION, KEY, ON, STR, WITH, STR).Handle(func(params Params) Statement {
		parts := strings.Split(params[0], ".")
		tags := strings.Split(params[1], ".")
		switch len(parts) {
		case 1:
			return CreatePartitionKeyStatement{parts[0], "", tags}
		case 2:
			return CreatePartitionKeyStatement{parts[0], parts[1], tags}
		}
		return nil
	})
	lang.Spec(CREATE, PARTITION, KEY, STR, ON, STR).Handle(func(params Params) Statement {
		parts := strings.Split(params[1], ".")
		tags := strings.Split(params[0], ".")
		switch len(parts) {
		case 1:
			return CreatePartitionKeyStatement{parts[0], "", tags}
		case 2:
			return CreatePartitionKeyStatement{parts[0], parts[1], tags}
		}
		return nil
	})
	lang.Spec(DROP, PARTITION, KEY, ON, STR).Handle(func(params Params) Statement {
		parts := strings.Split(params[0], ".")
		switch len(parts) {
		case 1:
			return DropPartitionKeyStatement{parts[0], ""}
		case 2:
			return DropPartitionKeyStatement{parts[0], parts[1]}
		}
		// dropping a partitiion key will not immidiately remove it. a task will be created for first importing the
		// data to all nodes that should have the data based on the new token assigned to the measurement.
		// there is a delay of about a minute before starting the import in case te user changes its mind
		return nil
	})
	// UPDATE PARTITION KEY ON
	// This is practically what drop partition key does as well. All data need to be downloaded and the partition
	// key tag updated before saveing the data in its new position.

	// SET REPLICATION FACTOR 3 ON "mydb.mymeasurement" lang.Spec(SET, REPLICATION, FACTOR, NUM, ON, STR)
	// SHOW REPLICATIONS FACTORS
	// REMOVE NODE "node3"
	return lang
}

type Params []string
type Handler func (Params) Statement

type Tree struct {
	Children map[Token]*Tree
	Handler  Handler
}

func newTree() *Tree {
	return &Tree{Children: map[Token]*Tree{}}
}

type Spec struct {
	tree *Tree
}

func (s *Spec) Handle(f Handler) {
	s.tree.Handler = f
}

type Language struct {
	trees map[Token]*Tree
}

func (l *Language) Spec(first Token, others ...Token) *Spec {
	if _, ok := l.trees[first]; !ok {
		l.trees[first] = newTree()
	}
	tree := l.trees[first]
	for _, t := range others {
		if _, ok := tree.Children[t]; !ok {
			tree.Children[t] = newTree()
		}
		tree = tree.Children[t]
	}
	return &Spec{tree}
}

func NewLanguage() *Language {
	return &Language{trees: map[Token]*Tree{}}
}