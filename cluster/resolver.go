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
	collection	PartitionCollection
}

// FindByKey can return multiple locations for replication and load balancing.
// On reads, it will not return nodes with status "syncing"
// However, on writes it will return "syncing" nodes so that they can catch up.
func (r *Resolver) FindByKey(key int, purpose int) []string {
	return []string{"not implemented"}
}

func (r *Resolver) FindAll() []string {
	return []string{"not implemented"}
}

func (r *Resolver) NotifyNewToken(token int, node *Node) {
	p := &Partition{token, node}
	r.collection.Put(p)
}

func (r *Resolver) NotifyRemovedToken(token int, node *Node) {
	// r.collection.Remove(token)
}