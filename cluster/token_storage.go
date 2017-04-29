package cluster

import (
	"context"
	"github.com/coreos/etcd/client"
	"log"
	"time"
	"strconv"
	"strings"
)

type tokenStorage interface {
	Assign()
	Get()
	Reserve()
	Release()
}

const tokenStorageBaseDir = "influxdbCluster"

type etcdTokenStorage struct {
	client client.Client
	kapi   client.KeysAPI
}

// Assign sets a token to refer to a certain node
func (s *etcdTokenStorage) Assign(token int, node string) error {
	val := node
	_, err := s.kapi.Set(context.Background(), createEtcdTokenPath(token), val, nil)
	return err
}

// Get return a map of all tokens and the nodes they refer to.
func (s *etcdTokenStorage) Get() (map[int]string, error) {
	resp, getErr := s.kapi.Get(context.Background(), tokenStorageBaseDir + "/reservedTokens/", nil)
	if getErr != nil {
		return nil, getErr
	}
	tokenMap := map[int]string{}
	for _, node := range resp.Node.Nodes {
		parts := strings.Split(node.Key, "/")
		token, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			tokenMap[token] = node.Value
		}
	}
	return tokenMap, nil
}

// Reserve should prevent other nodes from assigning a token to itself as some other is
// currently importing data for it to later assign the token to itself.
func (s *etcdTokenStorage) Reserve(token int, node string) (bool, error) {
	resp, getErr := s.kapi.Get(context.Background(), createEtcdTokenPath(token), nil)
	// Do not reserve if it is reserved by some other node
	if getErr != nil && getErr.(client.Error).Code != client.ErrorCodeKeyNotFound  {
		return false, getErr
	}
	if resp != nil && resp.Node.Value != node {
		return false, nil
	}
	val := node
	opts := client.SetOptions{}
	opts.TTL = time.Minute
	_, err := s.kapi.Set(context.Background(), createEtcdTokenPath(token), val, nil)
	return err == nil, err
}

// Release is called after a node has finished importing data for a token range
func (s *etcdTokenStorage) Release(token int) error {
	_, err := s.kapi.Delete(context.Background(), createEtcdTokenPath(token), nil)
	return err
}

func (s *etcdTokenStorage) Open(entrypoints []string) {
	c, err := client.New(client.Config{
		Endpoints:               entrypoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	})
	s.client = c
	if err != nil {
		log.Fatal(err)
	}
	kapi := client.NewKeysAPI(c)
	s.kapi = kapi
}

func createEtcdTokenPath(token int) string {
	return tokenStorageBaseDir + "/reservedTokens/" + strconv.Itoa(token)
}
