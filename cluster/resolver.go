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
}

func NewResolver() *Resolver {
	return &Resolver{NewPartitionCollection(), make(map[*Node]int)}
}

// FindByKey can return multiple locations for replication and load balancing.
// On reads, it will not return nodes with status "syncing"
// However, on writes it will return "syncing" nodes so that they can catch up.
func (r *Resolver) FindByKey(key int, purpose int) []string {
	partition := r.collection.Get(key)
	if partition != nil {
		return []string{partition.Node.DataLocation}
	}
	return []string{}
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
