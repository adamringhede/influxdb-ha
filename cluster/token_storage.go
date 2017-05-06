package cluster

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"log"
	"time"
	"strconv"
	"strings"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
)

type tokenStorage interface {
	Assign()
	Get()
	Reserve()
	Release()
}

const etcdStorageBaseDir = "influxdbCluster"
const etcdStorageReservedTokens = "reservedTokens"
const etcdStorageTokens = "tokens"

type etcdTokenStorage struct {
	client *clientv3.Client
	reservations map[int]clientv3.LeaseID
}

// Assign sets a token to refer to a certain node
func (s *etcdTokenStorage) Assign(token int, node string) error {
	resp, err := s.client.Put(context.Background(), createEtcdTokenPath(token, etcdStorageTokens), node)
	log.Println(resp)
	return err
}

// Get return a map of all tokens and the nodes they refer to.
func (s *etcdTokenStorage) Get() (map[int]string, error) {
	resp, getErr := s.client.Get(context.Background(), etcdStorageBaseDir+ "/" + etcdStorageTokens + "/", clientv3.WithPrefix())
	if getErr != nil {
		return nil, getErr
	}
	tokenMap := map[int]string{}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), "/")
		token, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			tokenMap[token] = string(kv.Value)
		}
	}
	return tokenMap, nil
}

// Reserve should prevent other nodes from assigning a token to itself as some other is
// currently importing data for it to later assign the token to itself.
func (s *etcdTokenStorage) Reserve(token int, node string) (bool, error) {
	resp, getErr := s.client.Get(context.Background(), createEtcdTokenPath(token, etcdStorageReservedTokens))
	// Do not reserve if it is reserved by some other node
	if getErr != nil {
		switch e := getErr.(type) {
		case rpctypes.EtcdError:
			if e != rpctypes.ErrKeyNotFound {
				return false, getErr
			}
		default:
			return false, getErr
		}
	}
	if resp != nil && resp.Count > 0 && string(resp.Kvs[0].Value) != node {
		return false, nil
	}
	val := node
	lease := clientv3.NewLease(s.client)
	// The node has 5 hours to import data from the other node, after that point another node can
	// take over the node. The reason for the lease is to avoid interrupting large transfers.
	grantResp, grantErr := lease.Grant(context.Background(), 3600 * 5)
	if grantErr != nil {
		return false, grantErr
	}
	s.reservations[token] = grantResp.ID
	opts := clientv3.WithLease(grantResp.ID)
	_, err := s.client.Put(context.Background(), createEtcdTokenPath(token, etcdStorageReservedTokens), val, opts)
	return err == nil, err
}

// Release is called after a node has finished importing data for a token range
func (s *etcdTokenStorage) Release(token int) error {
	if _, ok := s.reservations[token]; ok {
		s.client.Revoke(context.Background(), s.reservations[token])
	}
	_, err := s.client.Delete(context.Background(), createEtcdTokenPath(token, etcdStorageReservedTokens))
	return err
}

func NewEtcdTokenStorage() *etcdTokenStorage {
	s := &etcdTokenStorage{}
	s.reservations = make(map[int]clientv3.LeaseID)
	return s
}

func NewEtcdTokenStorageWithClient(c *clientv3.Client) *etcdTokenStorage {
	s := NewEtcdTokenStorage()
	s.client = c
	return s
}

func (s *etcdTokenStorage) Open(entrypoints []string) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   entrypoints,
		DialTimeout: 5 * time.Second,
	})
	s.client = c
	if err != nil {
		log.Fatal(err)
	}
}

func createEtcdTokenPath(token int, path string) string {
	return etcdStorageBaseDir + "/" + path + "/" + strconv.Itoa(token)
}
