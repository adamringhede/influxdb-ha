package cluster

const (
	WRITE = iota
	READ
)

// Resolver finds locations where data should be.
// It hides the underlying distributed architecture
// so that it can potentially be replaced to support
// a different one.
type Resolver struct {
	collection *PartitionCollection
	// nodes is a set of nodes. It is managed by AddToken and RemoveToken. It should never be
	// changed outside of those functions.
	nodes      map[*Node]int
	replicationFactor int
}

func NewResolver() *Resolver {
	return &Resolver{NewPartitionCollection(), make(map[*Node]int), 2}
}

// FindByKey can return multiple locations for replication and load balancing.
// On reads, it will not return nodes with status "syncing"
// However, on writes it will return "syncing" nodes so that they can catch up.
func (r *Resolver) FindByKey(key int, purpose int) []string {
	partitions := r.collection.GetMultiple(key, r.replicationFactor)
	locationMap := make(map[string]bool)
	for _, p := range partitions {
		locationMap[p.Node.DataLocation] = true
	}
	locations := []string{}
	for location, _ := range locationMap {
		locations = append(locations, location)
	}
	return locations
}

func (r *Resolver) GetPartition(key int) *Partition {
	return r.collection.Get(key)
}

func (r *Resolver) FindPrimary(key int) *Node {
	p := r.collection.Get(key)
	if p != nil {
		return p.Node
	}
	return nil
}

func (r *Resolver) ReverseSecondaryLookup(key int) []int {
	if r.replicationFactor == 1 {
		return []int{key}
	}
	// If the caller has a sorted list of keys, it is trivial to avoid an n^2 complexity by optimizing this.
	// and only iterate over the entire tree once.
	tokens := []int{}
	for _, p := range r.collection.tree.Values() {
		if p.(*Partition).Token == key {
			continue
		}
		targets := r.collection.GetMultiple(p.(*Partition).Token, r.replicationFactor)
		for _, other := range targets {
			if other.Token == key {
				tokens = append(tokens, p.(*Partition).Token)
			}
		}
	}
	return tokens
}

func (r *Resolver) FindAll() []string {
	locations := []string{}
	for node, _ := range r.nodes {
		locations = append(locations, node.DataLocation)
	}
	return locations
}

func (r *Resolver) AddToken(token int, node *Node) {
	p := &Partition{token, node}
	r.collection.Put(p)
	if _, ok := r.nodes[node]; !ok {
		r.nodes[node] = 0
	}
	r.nodes[node] = r.nodes[node] + 1
}

func (r *Resolver) RemoveToken(token int) {
	p := r.collection.Get(token)
	if p != nil {
		node := p.Node
		r.collection.Remove(token)
		if _, ok := r.nodes[node]; !ok {
			r.nodes[node] = r.nodes[node] - 1
			if r.nodes[node] <= 0 {
				delete(r.nodes, node)
			}
		}
	}
}
