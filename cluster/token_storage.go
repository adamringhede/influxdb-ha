package cluster

import (
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"log"
	"strconv"
	"strings"
	"time"
)

const maxToken = 2147483647

type tokenStorage interface {
	Assign()
	Get()
	Reserve()
	Release()
}

const etcdStorageBaseDir = "influxdbCluster"
const etcdStorageReservedTokens = "reservedTokens"
const etcdStorageTokens = "tokens"

type etcdStorageBase struct {
	clusterID string
	client    *clientv3.Client
}

func (s *etcdStorageBase) path(path string) string {
	return etcdStorageBaseDir + "/" + s.clusterID + "/" + path + "/"
}

type etcdTokenStorage struct {
	etcdStorageBase
	session      *concurrency.Session
	reservations map[int]clientv3.LeaseID
}

func (s *etcdTokenStorage) Watch() clientv3.WatchChan {
	return s.client.Watch(context.Background(), s.path(etcdStorageTokens), clientv3.WithPrefix())
}

func (s *etcdTokenStorage) Lock() (*concurrency.Mutex, error) {
	mtx := concurrency.NewMutex(s.session, s.path("reserving"))
	err := mtx.Lock(context.Background())
	if err != nil {
		return nil, err
	}
	return mtx, nil
}

func (s *etcdTokenStorage) SuggestReservations() ([]int, error) {
	currentTokens, err := s.Get()
	if err != nil {
		return nil, err
	}
	nodeTokens := map[string][]int{}
	for token, name := range currentTokens {
		_, ok := nodeTokens[name]
		if !ok {
			nodeTokens[name] = []int{}
		}
		nodeTokens[name] = append(nodeTokens[name], token)
	}
	// Steal tokens from other nodes
	avgTokens := len(currentTokens) / (len(nodeTokens) + 1)
	suggestions := []int{}
	for len(suggestions) < avgTokens {
		for name, tokens := range nodeTokens {
			suggestions = append(suggestions, tokens[0])
			nodeTokens[name] = tokens[2:]
		}
	}
	return suggestions, nil
}

// Assign sets a token to refer to a certain node
func (s *etcdTokenStorage) Assign(token int, node string) error {
	resp, err := s.client.Put(context.Background(), s.tokenPath(token, etcdStorageTokens), node)
	log.Println(resp)
	return err
}

// Get return a map of all tokens and the nodes they refer to.
func (s *etcdTokenStorage) Get() (map[int]string, error) {
	resp, getErr := s.client.Get(context.Background(), s.path(etcdStorageTokens), clientv3.WithPrefix())
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
	resp, getErr := s.client.Get(context.Background(), s.tokenPath(token, etcdStorageReservedTokens))
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
	if resp != nil && resp.Count > 0 {
		return string(resp.Kvs[0].Value) == node, nil
	}
	lease := clientv3.NewLease(s.client)
	// The node has 30 minutes to import data from the other node, after that point another node can
	// take over the token. The reason for the lease is to avoid interrupting large transfers.
	grantResp, grantErr := lease.Grant(context.Background(), 3600/2)
	if grantErr != nil {
		return false, grantErr
	}
	s.reservations[token] = grantResp.ID
	_, err := s.client.Put(context.Background(), s.tokenPath(token, etcdStorageReservedTokens), node,
		clientv3.WithLease(grantResp.ID))
	return err == nil, err
}

// Release is called after a node has finished importing data for a token range
func (s *etcdTokenStorage) Release(token int) error {
	if _, ok := s.reservations[token]; ok {
		s.client.Revoke(context.Background(), s.reservations[token])
	}
	_, err := s.client.Delete(context.Background(), s.tokenPath(token, etcdStorageReservedTokens))
	delete(s.reservations, token)
	return err
}

func (s *etcdTokenStorage) Init(node string, numRanges int) error {
	_, err := concurrency.NewSTM(s.client, func(stm concurrency.STM) error {
		initKey := s.path("initiated")
		initiated := stm.Get(initKey)
		if initiated == "true" {
			return errors.New("Already initiated")
		}
		rangeSize := maxToken / numRanges
		for i := 0; i < numRanges; i++ {
			stm.Put(s.tokenPath(i*rangeSize, etcdStorageTokens), node)
		}
		stm.Put(initKey, "true")
		return nil
	})
	return err
}

type errAlreadyInitiated struct{}

func (e errAlreadyInitiated) Error() string { return "Already initiated" }

func (s *etcdTokenStorage) InitMany(node string, numRanges int) (bool, error) {
	initKey := s.path("initiated")
	//mtx, lockErr := s.Lock()
	//if lockErr != nil {
	//	return false, lockErr
	//}
	//defer mtx.Unlock(context.Background())
	_, err := concurrency.NewSTM(s.client, func(stm concurrency.STM) error {
		initiated := stm.Get(initKey)
		if initiated == "true" {
			return errAlreadyInitiated{}
		}
		lease := clientv3.NewLease(s.client)
		grantResp, err := lease.Grant(context.Background(), 30)
		if err != nil {
			return err
		}
		reserved := stm.Get(s.tokenPath(0, etcdStorageReservedTokens))
		if reserved != "" {
			return errAlreadyInitiated{}
		}
		stm.Put(s.tokenPath(0, etcdStorageReservedTokens), node,
			clientv3.WithLease(grantResp.ID))
		stm.Put(initKey, "true")
		return nil
	})
	if err != nil {
		switch e := err.(type) {
		case errAlreadyInitiated:
			return false, nil
		default:
			return false, e
		}
	}
	// we are first to initiate the cluster and no other node should ever get to this point unless we fail
	rangeSize := maxToken / numRanges
	for i := 0; i < numRanges; i++ {
		err := s.Assign(i*rangeSize, node)
		if err != nil {
			// Attempt to clean up
			s.client.Delete(context.Background(), initKey)
			return false, err
		}
	}
	s.Release(0)
	return true, nil
}

func NewEtcdTokenStorage() *etcdTokenStorage {
	s := &etcdTokenStorage{}
	s.reservations = make(map[int]clientv3.LeaseID)
	return s
}

func NewEtcdTokenStorageWithClient(c *clientv3.Client) *etcdTokenStorage {
	s := NewEtcdTokenStorage()
	s.client = c
	session, err := concurrency.NewSession(s.client)
	if err != nil {
		log.Fatal(err)
	}
	s.session = session
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
	session, err := concurrency.NewSession(s.client)
	if err != nil {
		log.Fatal(err)
	}
	s.session = session
}

func (s *etcdTokenStorage) Close() {
	s.client.Close()
}

func (s *etcdTokenStorage) tokenPath(token int, path string) string {
	return s.path(path) + strconv.Itoa(token)
}
