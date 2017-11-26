package clusterql

type Statement interface{}

type ShowPartitionKeysStatement struct {
	Database    string
	Measurement string
}

type DropPartitionKeyStatement struct {
	Database    string
	Measurement string
}

type CreatePartitionKeyStatement struct {
	Database    string
	Measurement string
	Tags        []string
}

type ShowReplicationFactorsStatement struct {
	Database    string
	Measurement string
}

type SetReplicationFactorStatement struct {
	Database    string
	Measurement string
	Value int
}

type RemoveNodeStatement struct {
	Name string
}