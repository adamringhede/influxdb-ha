package cluster

const (
	PURPOSE_WRITE = iota
	PURPOSE_READ
)

// Resolver finds locations where data should be.
// It hides the underlying distributed architecture
// so that it can potentially be replaced to support
// a different one.
type Resolver struct {
	collection	PartitionCollection
}

func (r *Resolver) FindByKey(key int, purpose int) []string {
	return []string{"not implemented"}
}

func (r *Resolver) FindAll() []string {
	return []string{"not implemented"}
}

func (r *Resolver) NotifyNewToken(token int, node *Node) {
	// create a new partition
	//partition := Partition{token, node}
	// r.collection.Put(partition)
}

func (r *Resolver) NotifyRemovedToken(token int, node *Node) {
	// r.collection.Remove(token)
}