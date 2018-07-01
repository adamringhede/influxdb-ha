package cluster

import (
	"github.com/coreos/etcd/clientv3"
	"strings"
)

const etcdStorageBaseDir = "influxdbCluster"
const etcdStorageReservedTokens = "reservedTokens"

type EtcdStorageBase struct {
	ClusterID string
	Client    *clientv3.Client
}

func (s *EtcdStorageBase) path(paths ...string) string {
	return etcdStorageBaseDir + "/" + s.ClusterID + "/" + strings.Join(paths, "/") + "/"
}