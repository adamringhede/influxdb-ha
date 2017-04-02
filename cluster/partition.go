package cluster

type Partition struct {
	Token	int
	Node	*Node
}

// PartitionCollection is an abstraction for partition look ups
// backed by an efficient data structure
type PartitionCollection struct {

}