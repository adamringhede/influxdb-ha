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
	collection	*PartitionCollection
}

func NewResolver() *Resolver {
	return &Resolver{NewPartitionCollection()}
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
	for _, value := range r.collection.tree.Values(){
		locations = append(locations, value.(*Partition).Node.DataLocation)
	}
	return locations
}

func (r *Resolver) AddToken(token int, node *Node) {
	p := &Partition{token, node}
	r.collection.Put(p)
}

func (r *Resolver) RemoveToken(token int) {
	r.collection.Remove(token)
}