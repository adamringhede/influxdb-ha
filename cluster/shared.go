package cluster

import (
	"context"
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

func DestroyCluster(clusterID string, clnt *clientv3.Client) (err error) {
	_, err = clnt.Delete(context.Background(), etcdStorageBaseDir + "/" + clusterID, clientv3.WithPrefix())
	return
}