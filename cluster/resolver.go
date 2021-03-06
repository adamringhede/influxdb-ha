package cluster

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
)

type ResolvePurpose int

const (
	WRITE ResolvePurpose = iota
	READ
)

// Resolver finds locations where data should be.
// It hides the underlying distributed architecture
// so that it can potentially be replaced to support
// a different one.
type Resolver struct {
	// TODO add collection for reserved tokens which are used when writing
	collection *PartitionCollection
	nodes             NodeCollection
	ReplicationFactor int
}

func NewResolver() *Resolver {
	return NewResolverWithNodes(NewLocalNodeCollection())
}

func NewResolverWithNodes(nodes NodeCollection) *Resolver {
	return &Resolver{NewPartitionCollection(), nodes, 2}
}

func (r *Resolver) PrintRing() {
	it := r.collection.tree.Iterator()
	for it.Next() {
		token := it.Key().(int)
		partition := it.Value().(*Partition)
		fmt.Printf("%d -> %s \n", token, partition.Node.Name)
	}
}

func (r *Resolver) FindTokenByKey(key int) (int, bool) {
	partition, ok := r.collection.GetPartition(key)
	if !ok {
		return 0, ok
	}
	return partition.Token, ok
}

func (r *Resolver) FindNodesByKey(key int, purpose ResolvePurpose) []*Node {
	partitions := r.collection.GetMultiple(key, r.ReplicationFactor)
	nodesMap := make(map[*Node]bool)
	for _, p := range partitions {
		// Getting node from the nodes collection instead as the one in the
		// partition may be out of date.
		node, nodeExists := r.nodes.Get(p.Node.Name)
		if nodeExists && purpose == READ && node.Status != NodeStatusUp {
			// If a token is assigned to a node
			continue
		}
		nodesMap[&node] = true
	}
	nodes := []*Node{}
	for node := range nodesMap {
		nodes = append(nodes, node)
	}
	return nodes
}

// FindByKey can return multiple locations for replication and load balancing.
// On reads, it will not return nodes with status "recovering"
// However, on writes it will return recoverings nodes so that they can catch up.
func (r *Resolver) FindByKey(key int, purpose ResolvePurpose) []string {
	locations := []string{}
	for _, node := range r.FindNodesByKey(key, purpose) {
		locations = append(locations, node.DataLocation)
	}
	return locations
}

func (r *Resolver) GetPartition(key int) *Partition {
	return r.collection.Get(key)
}

func (r *Resolver) FindPrimary(key int) *Node {
	p := r.collection.Get(key)
	if p != nil {
		if node, nodeExists := r.nodes.Get(p.Node.Name); nodeExists {
			return &node
		}
	}
	return nil
}

func (r *Resolver) ReverseSecondaryLookup(key int) []int {
	if r.ReplicationFactor == 1 {
		return []int{key}
	}
	// If the caller has a sorted list of keys, it is trivial to avoid an n^2 complexity by optimizing this.
	// and only iterate over the entire tree once.
	tokens := []int{}
	for _, p := range r.collection.tree.Values() {
		if p.(*Partition).Token == key {
			continue
		}
		targets := r.collection.GetMultiple(p.(*Partition).Token, r.ReplicationFactor)
		for _, other := range targets {
			if other.Token == key {
				tokens = append(tokens, p.(*Partition).Token)
			}
		}
	}
	return tokens
}

func (r *Resolver) FindAllNodes() []*Node {
	nodes := []*Node{}
	for _, node := range r.nodes.GetAll() {
		nodes = append(nodes, &node)
	}
	return nodes
}

// FindAll returns the data locattions of all nodes in the cluster
func (r *Resolver) FindAll() []string {
	locations := []string{}
	for _, node := range r.nodes.GetAll() {
		locations = append(locations, node.DataLocation)
	}
	return locations
}

// RemoveAllTokens clears tll tokens. This is not thread safe.
func (r *Resolver) RemoveAllTokens() {
	r.collection.Clear()
}

func (r *Resolver) AddToken(token int, node *Node) {
	p := &Partition{token, node} // if node is nil, this can cause issues later on when fetching data.
	r.collection.Put(p)
	if node != nil {
		if _, exists := r.nodes.Get(node.Name); !exists {
			r.nodes.Persist(*node)
		}
	}
}

func (r *Resolver) RemoveToken(token int) {
	p := r.collection.Get(token)
	if p != nil {
		r.collection.Remove(token)
	}
}

type ResolverSyncer struct {
	resolver *Resolver
	tokens   *EtcdTokenStorage
	nodes    NodeCollection
	closeCh  chan bool
}

func (r *ResolverSyncer) trackUpdates() {
	// Periodically
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case update := <-r.tokens.Watch():
			for _, event := range update.Events {
				keyParts := strings.Split(string(event.Kv.Key), "/")
				token, err := strconv.Atoi(keyParts[len(keyParts)-1])
				nodeName := string(event.Kv.Value)
				if err != nil {
					log.Printf("Failed to parse token %s", event.Kv.Key)
					continue
				}
				if event.Type == mvccpb.PUT {
					node, ok := r.nodes.Get(nodeName)
					if ok {
						r.resolver.AddToken(token, &node)
					}
				}
				if event.Type == mvccpb.DELETE {
					r.resolver.RemoveToken(token)
				}
			}
			case <-ticker.C:
				r.updateFromStorage()
		case <-r.closeCh:
			return
		}
	}
}

// updateFromStorage updates the assignment of tokens to nodes.
func (c *ResolverSyncer) updateFromStorage() error {
	tokens, err := c.tokens.Get()
	if err != nil {
		return err
	}
	for token, nodeName := range tokens {
		if node, ok := c.nodes.Get(nodeName); ok {
			c.resolver.AddToken(token, &node)
		} else {
			// TODO Fix this message so that we don't log it for every token as there may be thousands of them .
			//fmt.Printf("Warning: Could not find a node with name '%s'. This token should be reassigned to another node before removing the node.", nodeName)
			// setting the node to nil is not a good idea. this can cause other things to fail later
			//c.resolver.AddToken(token, nil)
		}
	}
	return nil
}

func (c *ResolverSyncer) Close() {
	close(c.closeCh)
}

// NewSyncedResolver fetches existing tokens from storage
func NewResolverSyncer(resolver *Resolver, tokens *EtcdTokenStorage, nodes NodeCollection) (*ResolverSyncer, error) {
	c := &ResolverSyncer{resolver: resolver, tokens: tokens, nodes: nodes, closeCh: make(chan bool)}
	err := c.updateFromStorage()
	if err != nil {
		return nil, err
	}
	go c.trackUpdates()
	return c, nil
}
