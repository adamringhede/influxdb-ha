package syncing

type RetentionPolicy struct {
	Name               string
	Duration           string
	ShardGroupDuration string
	Replicas           int
	Default            bool
}

type ContinuousQuery struct {
	Name  string
	Query string
}
