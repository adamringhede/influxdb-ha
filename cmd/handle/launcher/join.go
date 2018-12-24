package launcher

import (
	"context"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/syncing"
	"log"
	"strconv"
	"strings"
	"sync"
)

func tokensToString(tokens []int, sep string) string {
	res := make([]string, len(tokens))
	for i, token := range tokens {
		res[i] = strconv.Itoa(token)
	}
	return strings.Join(res, sep)
}

func Join(localNode *cluster.Node, tokenStorage cluster.LockableTokenStorage, nodeStorage cluster.NodeStorage, resolver *cluster.Resolver, importer syncing.Importer) error {
	mtx, err := tokenStorage.Lock()
	if err != nil {
		return err
	}
	defer mtx.Unlock(context.Background())

	isFirstNode, err := tokenStorage.InitMany(localNode.Name, 512) // this may have failed for the first node.
	if err != nil {
		return err
	}
	if !isFirstNode {
		err = joinExisting(localNode, tokenStorage, resolver, importer)
		if err != nil {
			return err
		}
	}
	localNode.Status = cluster.NodeStatusUp
	err = nodeStorage.Save(localNode)
	// If this fails, the node will be stuck in the wrong state unable to receive writes
	return err
}

// joinExisting takes tokens belonging to other nodes and starts importing data. This function is idempotent and can be called on multiple
func joinExisting(localNode *cluster.Node, tokenStorage cluster.LockableTokenStorage, resolver *cluster.Resolver, importer syncing.Importer) error {
	toSteal, err := cluster.SuggestReservationsDistributed(tokenStorage, resolver)
	log.Printf("Stealing %d tokens: [%s]", len(toSteal), tokensToString(toSteal, " "))
	if err != nil {
		return err
	}
	var reserved []int
	for _, tokenToSteal := range toSteal {

		ok, err := tokenStorage.Reserve(tokenToSteal, localNode.Name)
		if err != nil {
			return err
		}
		if ok {
			reserved = append(reserved, tokenToSteal)
		}
	}

	log.Println("Starting import of primary data")
	importer.ImportPartitioned(reserved, localNode.DataLocation)

	oldDataHolders := map[int][]*cluster.Node{}
	for _, token := range reserved {
		oldDataHolders[token] = resolver.FindNodesByKey(token, cluster.WRITE)
		for _, secondary := range resolver.ReverseSecondaryLookup(token) {
			oldDataHolders[secondary] = resolver.FindNodesByKey(secondary, cluster.WRITE)
		}
	}
	for _, token := range reserved {
		err = tokenStorage.Release(token)
		if err != nil {
			panic(err)
		}
		err = tokenStorage.Assign(token, localNode.Name)
		if err != nil {
			panic(err)
		}

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
		importer.ImportPartitioned(secondaryTokens, localNode.DataLocation)
	}

	// Importing non partitioned after tokens been assigned to get primary and secondary data.
	importer.ImportNonPartitioned(localNode.DataLocation)

	// The filtered list of primaries which not longer should hold data for assigned tokens.
	deleteMap := map[int]*cluster.Node{}
	for token, nodes := range oldDataHolders {
		for _, node := range nodes {
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
	}
	deleteTokensData(deleteMap, importer)
	return nil
}


func deleteTokensData(tokenLocations map[int]*cluster.Node, importer syncing.Importer) {
	// This will try to delete data on the node if it is available. If it is unavailable, it should be responsible
	// to delete data it should not have during its recovery process.
	g := sync.WaitGroup{}
	g.Add(len(tokenLocations))
	for token, node := range tokenLocations {
		go (func(token int, node *cluster.Node) {
			err := importer.DeleteByToken(node.DataLocation, token)
			if err != nil {
				log.Printf("Failed to delete data at %s (%s) with token %d\n", node.Name, node.DataLocation, token)
			}
			g.Done()
		})(token, node)
	}
	g.Wait()
}
