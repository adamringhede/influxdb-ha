package clusterql

import "github.com/influxdata/influxql"

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
	Value       int
}

type ShowNodesStatement struct{}

type RemoveNodeStatement struct {
	Name string
}

// FIXME This may not be necessary. We do not need this in clusterql as it is already supported in InfluxDB.
// Instead we can just pass the query to the auth service in the query handler!!
type SetPasswordUserStatement struct {
	influxql.SetPasswordUserStatement
}

type RevokeStatement struct {
	influxql.RevokeStatement
}

type RevokeAdminStatement struct{ influxql.RevokeAdminStatement }

type GrantStatement struct {
	influxql.GrantStatement
}

type GrantAdminStatement struct {
	influxql.GrantAdminStatement
}

type CreateUserStatement struct {
	influxql.CreateUserStatement
}

type DropUserStatement struct {
	influxql.DropUserStatement
}
