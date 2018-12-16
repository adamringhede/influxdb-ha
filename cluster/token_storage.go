package cluster

import (
	"context"
	"errors"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
)

// The max token is the maximum value a token could have as hashes used for partitioning are represented using uint32
const maxToken = math.MaxUint32

type TokenStorage interface {
	Assign(token int, node string) error
	Get() (map[int]string, error)
	Reserve(token int, node string) (bool, error)
	Release(token int) error
	InitMany(node string, numRanges int) (bool, error)
}

type LockableTokenStorage interface {
	TokenStorage
	Lock() (*concurrency.Mutex, error)
}

const etcdStorageTokens = "tokens"

type EtcdTokenStorage struct {
	EtcdStorageBase
	session      *concurrency.Session
	reservations map[int]clientv3.LeaseID
}

func (s *EtcdTokenStorage) Watch() clientv3.WatchChan {
	return s.Client.Watch(context.Background(), s.path(etcdStorageTokens), clientv3.WithPrefix())
}

func (s *EtcdTokenStorage) Lock() (*concurrency.Mutex, error) {
	mtx := concurrency.NewMutex(s.session, s.path("reserving_tokens_init"))
	err := mtx.Lock(context.Background())
	if err != nil {
		return nil, err
	}
	return mtx, nil
}


func SuggestReservations(tokenStorage TokenStorage) ([]int, error) {
	currentTokens, err := tokenStorage.Get()
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
	for _, tokens := range nodeTokens {
		// The only reason for sorting, is to avoid randomness so that tests can make assumptions
		// on token assignment.
		sort.Ints(tokens)
	}
	// Steal tokens from other nodes
	avgTokens := len(currentTokens) / (len(nodeTokens) + 1)
	suggestions := []int{}
	for len(suggestions) < avgTokens {
		for name, tokens := range nodeTokens {
			suggestions = append(suggestions, tokens[0])
			nodeTokens[name] = tokens[1:]
		}
	}
	return suggestions, nil
}

func SuggestReservationsDistributed(tokenStorage TokenStorage, resolver *Resolver) ([]int, error) {
	currentTokens, err := tokenStorage.Get()
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
	for _, tokens := range nodeTokens {
		// The only reason for sorting, is to avoid randomness so that tests can make assumptions
		// on token assignment.
		sort.Ints(tokens)
	}
	// Steal tokens from other nodes
	avgTokens := len(currentTokens) / (len(nodeTokens) + 1)
	suggestions := []int{}

	rangeSize := maxToken / avgTokens
	for i := 0; i < avgTokens; i++ {
		if token, ok := resolver.FindTokenByKey(i*rangeSize + 5); ok {
			suggestions = append(suggestions, token)
		}
	}
	return suggestions, nil
}

// Assign sets a token to refer to a certain node id
func (s *EtcdTokenStorage) Assign(token int, node string) error {
	_, err := s.Client.Put(context.Background(), s.tokenPath(token, etcdStorageTokens), node)
	return err
}

// Get return a map of all tokens and the nodes they refer to.
func (s *EtcdTokenStorage) Get() (map[int]string, error) {
	// TODO Try again if it fails.
	resp, getErr := s.Client.Get(context.Background(), s.path(etcdStorageTokens),
		clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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
func (s *EtcdTokenStorage) Reserve(token int, node string) (bool, error) {
	resp, getErr := s.Client.Get(context.Background(), s.tokenPath(token, etcdStorageReservedTokens))
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
	lease := clientv3.NewLease(s.Client)
	// The node has 30 minutes to import data from the other node, after that point another node can
	// take over the token. The reason for the lease is to avoid interrupting large transfers.
	grantResp, grantErr := lease.Grant(context.Background(), 1800)
	if grantErr != nil {
		return false, grantErr
	}
	s.reservations[token] = grantResp.ID
	_, err := s.Client.Put(context.Background(), s.tokenPath(token, etcdStorageReservedTokens), node,
		clientv3.WithLease(grantResp.ID))
	return err == nil, err
}

// Release is called after a node has finished importing data for a token range
func (s *EtcdTokenStorage) Release(token int) error {
	if _, ok := s.reservations[token]; ok {
		s.Client.Revoke(context.Background(), s.reservations[token])
	}
	_, err := s.Client.Delete(context.Background(), s.tokenPath(token, etcdStorageReservedTokens))
	delete(s.reservations, token)
	return err
}

func (s *EtcdTokenStorage) Clear() error {
	s.Client.Delete(context.Background(), s.path("initiated"), clientv3.WithPrefix())
	_, err := s.Client.Delete(context.Background(), s.path(etcdStorageTokens), clientv3.WithPrefix())
	return err
}

func (s *EtcdTokenStorage) Init(node string, numRanges int) error {
	_, err := concurrency.NewSTM(s.Client, func(stm concurrency.STM) error {
		initKey := s.path("initiated")
		initiated := stm.Get(initKey)
		if initiated == "true" {
			return errors.New("already initiated")
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

func (s *EtcdTokenStorage) InitMany(node string, numRanges int) (bool, error) {
	initKey := s.path("initiated")
	_, err := concurrency.NewSTM(s.Client, func(stm concurrency.STM) error {
		initiated := stm.Get(initKey)
		if initiated == "true" {
			return errAlreadyInitiated{}
		}
		lease := clientv3.NewLease(s.Client)
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
		switch err.(type) {
		case errAlreadyInitiated:
			return false, nil
		default:
			return false, err
		}
	}
	log.Println("Initiating tokens")
	// we are first to initiate the cluster and no other node should ever get to this point unless we fail
	rangeSize := maxToken / numRanges
	for i := 0; i < numRanges; i++ {
		err := s.Assign(i*rangeSize, node)
		if err != nil {
			// Attempt to clean up
			s.Client.Delete(context.Background(), initKey)
			return false, err
		}
	}
	s.Release(0)
	return true, nil
}

func NewEtcdTokenStorage() *EtcdTokenStorage {
	s := &EtcdTokenStorage{}
	s.reservations = make(map[int]clientv3.LeaseID)
	return s
}

func NewEtcdTokenStorageWithClient(c *clientv3.Client) *EtcdTokenStorage {
	s := NewEtcdTokenStorage()
	s.Client = c
	session, err := concurrency.NewSession(s.Client)
	if err != nil {
		log.Fatal(err)
	}
	s.session = session
	return s
}

func (s *EtcdTokenStorage) Open(entrypoints []string) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   entrypoints,
		DialTimeout: 5 * time.Second,
	})
	s.Client = c
	if err != nil {
		log.Fatal(err)
	}
	session, err := concurrency.NewSession(s.Client)
	if err != nil {
		log.Fatal(err)
	}
	s.session = session
}

func (s *EtcdTokenStorage) Close() {
	s.Client.Close()
}

func (s *EtcdTokenStorage) tokenPath(token int, path string) string {
	return s.path(path) + strconv.Itoa(token)
}
