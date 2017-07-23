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
	"github.com/adamringhede/influxdb-ha/syncing"
	"sync"
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

	// TODO Only default to hostname, prefer using a configurable id
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

	nodes, err := nodeStorage.GetAll()
	handleErr(err)

	tokensMap, err := tokenStorage.Get()
	handleErr(err)

	// store nodes as a map
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
	// Starting the service here so that the node can receive writes while joining.
	go service.Start(resolver, partitioner, httpConfig)

	if isNew {
		mtx, err := tokenStorage.Lock()
		handleErr(err)
		isFirstNode, err := tokenStorage.InitMany(localNode.Name, 16)
		if err != nil {
			log.Println("Intitation of tokens failed")
			handleErr(err)
		}
		if !isFirstNode {
			err = join(localNode, tokenStorage, resolver)
			handleErr(err)
		}
		mtx.Unlock(context.Background())
	} else {
		// TODO check if importing data, if so, then resume.
	}

	// Sleep forever
	select {}
}

func tokensToString(tokens []int, sep string) string {
	res := make([]string, len(tokens))
	for i, token := range tokens {
		res[i] = strconv.Itoa(token)
	}
	return strings.Join(res, sep)
}

func join(localNode *cluster.Node, tokenStorage *cluster.EtcdTokenStorage, resolver *cluster.Resolver) error {
	toSteal, err := tokenStorage.SuggestReservations()
	log.Printf("Stealing %d tokens: [%s]", len(toSteal), tokensToString(toSteal, " "))
	if err != nil {
		return err
	}
	handleErr(err)
	reserved := []int{}
	for _, tokenToSteal := range toSteal {

		ok, err := tokenStorage.Reserve(tokenToSteal, localNode.Name)
		if err != nil {
			return err
		}
		if ok {
			reserved = append(reserved, tokenToSteal)
		}
		// TODO handle not ok
	}

	importer := syncing.BasicImporter{}
	log.Println("Starting import of primary data")
	importer.Import(reserved, resolver, localNode.DataLocation)

	oldPrimaries := map[int]*cluster.Node{}
	for _, token := range reserved {
		oldPrimaries[token] = resolver.FindPrimary(token)
		tokenStorage.Release(token)
		tokenStorage.Assign(token, localNode.Name)

		// Update the resolver with the most current assignments.
		resolver.AddToken(token, localNode)
	}

	// This takes one token and finds what tokens are also replicated to to the same node this node is assigned to.
	// This can only be done after assigning the tokens as the resolver needs to understand which
	// nodes tokens are allocated to, as the logic is skipping tokens assigned to the same node.
	secondaryTokens := []int{}
	for _, token := range reserved {
		secondaryTokens = append(secondaryTokens, resolver.ReverseSecondaryLookup(token)...)
	}
	if len(secondaryTokens) > 0 {
		log.Println("Starting import of replicated data")
		importer.Import(secondaryTokens, resolver, localNode.DataLocation)
	}

	// The filtered list of primaries which not longer should hold data for assigned tokens.
	deleteMap := map[int]*cluster.Node{}
	for token, node := range oldPrimaries {
		// check if the token still resolves the location
		// if not, the data should be deleted
		shouldDelete := true
		for _, replLoc := range resolver.FindByKey(token, cluster.WRITE) {
			if replLoc == node.DataLocation {
				shouldDelete = false
			}
		}
		if shouldDelete {
			deleteMap[token] = node
		}
	}
	deleteTokensData(deleteMap)
	return nil
}

func deleteTokensData(tokenLocations map[int]*cluster.Node) {
	/*
	this should just add a job in a queue to be picked up by the agent running at that node.
	that is if we want to be able to add a new node while another one is unavailable.
	if this is not a requirement, we can just make the delete request here.
	The danger with having the same data on multiple locations without intended replication,
	queries merging data from multiple nodes may receive incorrect results.
	This could however be avoided by filtering on partitionToken for those that should be on that
	node. An alternative is to have a background job that clears out data from nodes where it should not be.
	*/
	g := sync.WaitGroup{}
	g.Add(len(tokenLocations))
	for token, node := range tokenLocations {
		go (func() {
			syncing.Delete(token, node.DataLocation)
			g.Done()
		})()
	}
	g.Wait()
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
