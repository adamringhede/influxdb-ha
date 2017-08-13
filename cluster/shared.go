package cluster

import "github.com/coreos/etcd/clientv3"

const etcdStorageBaseDir = "influxdbCluster"
const etcdStorageReservedTokens = "reservedTokens"

type EtcdStorageBase struct {
	ClusterID string
	Client    *clientv3.Client
}

func (s *EtcdStorageBase) path(path string) string {
	return etcdStorageBaseDir + "/" + s.ClusterID + "/" + path + "/"
}