package main

import (
	"flag"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/service"
	"github.com/coreos/etcd/clientv3"
	"log"
	"os"
	"strings"
	"time"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"strconv"
	"context"
	"encoding/json"
)

type controller struct {
	resolver *cluster.Resolver
}

func (c *controller) NotifyNewToken(token int, node *cluster.Node) {
	c.resolver.AddToken(token, node)
}

func (c *controller) NotifyRemovedToken(token int, node *cluster.Node) {
	c.resolver.RemoveToken(token)
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	bindClientAddr := flag.String("client-addr", "0.0.0.0", "IP addres for client http requests")
	bindClientPort := flag.Int("client-port", 8086, "Port for http requests")
	data := flag.String("data", ":28086", "InfluxDB database port")
	etcdEndpoints := flag.String("etcd", "", "Comma seperated locations of etcd nodes")
	flag.Parse()

	c, etcdErr := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	handleErr(etcdErr)

	nodeName, hostErr := os.Hostname()
	handleErr(hostErr)
	nodeStorage := cluster.NewEtcdNodeStorage(c)
	tokenStorage := cluster.NewEtcdTokenStorageWithClient(c)

	localNode, nodeErr := nodeStorage.Get(nodeName)
	handleErr(nodeErr)
	isNew := localNode == nil
	if localNode == nil {
		localNode = &cluster.Node{}
		localNode.Name = nodeName
	}
	localNode.DataLocation = *data
	saveErr := nodeStorage.Save(localNode)
	handleErr(saveErr)

	if isNew {
		mtx, err := tokenStorage.Lock()
		handleErr(err)
		defer mtx.Unlock(context.Background())

		initiated, err := tokenStorage.InitMany(localNode.Name, 16)
		if err != nil {
			log.Println("Intitation of tokens failed")
		}
		handleErr(err)
		if !initiated {
			toSteal, err := tokenStorage.SuggestReservations()
			log.Printf("Stealing %d tokens", len(toSteal))
			log.Println(toSteal)
			if err != nil {
				mtx.Unlock(context.Background())
			}
			handleErr(err)
			reserved := []int{}
			for _, tokenToSteal := range toSteal {

				ok, err := tokenStorage.Reserve(tokenToSteal, localNode.Name)
				if err != nil {
					mtx.Unlock(context.Background())
					handleErr(err)
				}
				if ok {
					reserved = append(reserved, tokenToSteal)
				}
			}

			// TODO Need to import data for the tokens before unlocking
			// or else another node can try to reserve the same tokens
			// and therefore only get a few.

			/*
			For each token, get preceding tokens based on replication factor
			Instantiate an importer that resolves the nodes for each token
			and starts reading the data in limited series and regularly checkpoints.
			All read data should be written to the local node's data location.
			 */

			for _, token := range reserved {
				tokenStorage.Release(token)
				tokenStorage.Assign(token, localNode.Name)
				// Create a backlog items for the primary nodes to delete their data for each stolen
				// token.
			}

		} else {
			log.Println("Initiated tokens")
		}
		mtx.Unlock(context.Background())
	} else {
		// TODO check if importing data, if so, then resume.
	}

	nodes, err := nodeStorage.GetAll()
	handleErr(err)

	tokensMap, err := tokenStorage.Get()
	handleErr(err)

	// get nodes as a map
	nodesMap := make(map[string]*cluster.Node, len(nodes))
	for _, node := range nodes {
		nodesMap[node.Name] = node
	}

	resolver := cluster.NewResolver()
	for token, nodeName := range tokensMap {
		if node, ok := nodesMap[nodeName]; ok {
			resolver.AddToken(token, node)
		} else {
			log.Fatalf("Could not find a node with name %s", nodeName)
		}
	}

	// Watch for changes to nodes
	go (func() {
		for update := range nodeStorage.Watch() {
			for _, event := range update.Events {
				if event.Type == mvccpb.PUT {
					var node cluster.Node
					err := json.Unmarshal(event.Kv.Value, &node)
					if err != nil {
						log.Println("Failed to parse node from update: ", string(event.Kv.Value))
					}
					if _, ok := nodesMap[node.Name]; ok {
						nodesMap[node.Name].Status = node.Status
						nodesMap[node.Name].DataLocation = node.DataLocation
					} else {
						nodesMap[node.Name] = &node
					}

				}
			}
		}
	})()

	// Watch for changes to tokens and keep resolver in sync.
	go (func() {
		for update := range tokenStorage.Watch() {
			for _, event := range update.Events {
				keyParts := strings.Split(string(event.Kv.Key), "/")
				token, err := strconv.Atoi(keyParts[len(keyParts)-1])
				nodeName := string(event.Kv.Value)
				if err != nil {
					log.Printf("Failed to parse token %s", event.Kv.Key)
					continue
				}
				if event.Type == mvccpb.PUT {
					node, ok := nodesMap[nodeName]
					if !ok {
						foundNode, _ := nodeStorage.Get(nodeName)
						if foundNode != nil {
							nodesMap[nodeName] = foundNode
							node = foundNode
							ok = true
						}
					}
					if ok {
						resolver.AddToken(token, node)
					}
				}
				if event.Type == mvccpb.DELETE {
					resolver.RemoveToken(token)
				}
			}
		}
	})()

	httpConfig := service.Config{
		BindAddr: *bindClientAddr,
		BindPort: *bindClientPort,
	}

	// TODO get partitioner from a database and listen for changes which should be reflected here.
	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(cluster.PartitionKey{
		Database:    "sharded",
		Measurement: "treasures",
		Tags:        []string{"type"},
	})
	service.Start(resolver, partitioner, httpConfig)
}

func main2() {
	bindAddr := flag.String("addr", "0.0.0.0", "IP addres for listening on cluster communication")
	bindPort := flag.Int("port", 8084, "Port for listening on cluster communication")
	bindClientAddr := flag.String("client-addr", "0.0.0.0", "IP addres for client http requests")
	bindClientPort := flag.Int("client-port", 8086, "Port for http requests")
	data := flag.String("data", ":28086", "InfluxDB database port")
	join := flag.String("join", "", "Comma seperated locations of other nodes")
	flag.Parse()

	clusterConfig := cluster.Config{
		BindAddr:     *bindAddr,
		BindPort:     *bindPort,
		DataLocation: *data,
	}
	handle := createClusterHandle(clusterConfig, join)

	resolver := cluster.NewResolver()
	handle.TokenDelegate = &controller{resolver}
	for _, token := range handle.LocalNode.Tokens {
		handle.TokenDelegate.NotifyNewToken(token, &handle.LocalNode.Node)
	}

	httpConfig := service.Config{
		BindAddr: *bindClientAddr,
		BindPort: *bindClientPort,
	}
	// TODO get partitioner from a database and listen for changes which should be reflected here.
	partitioner := cluster.NewPartitioner()
	partitioner.AddKey(cluster.PartitionKey{
		Database:    "sharded",
		Measurement: "treasures",
		Tags:        []string{"type"},
	})
	service.Start(resolver, partitioner, httpConfig)
}

func createClusterHandle(clusterConfig cluster.Config, join *string) *cluster.Handle {
	handle, err := cluster.NewHandle(clusterConfig)
	if err != nil {
		panic(err)
	}
	others := strings.Split(*join, ",")
	if len(others) > 0 && *join != "" {
		log.Printf("Joining: %s", *join)
		joinErr := handle.Join(others)
		if joinErr != nil {
			log.Println("Failed to join any other node")
		}
	}
	printHostname()
	return handle
}

func printHostname() {
	hostname, nameErr := os.Hostname()
	if nameErr != nil {
		panic(nameErr)
	}
	log.Printf("Hostname: %s", hostname)
}
