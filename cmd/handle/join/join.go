package join

import (
	"log"
	"github.com/adamringhede/influxdb-ha/cluster"
	"sync"
	"github.com/adamringhede/influxdb-ha/syncing"
	"strings"
	"strconv"
	"context"
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

	isFirstNode, err := tokenStorage.InitMany(localNode.Name, 16) // this may have failed for the first node.
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
	toSteal, err := cluster.SuggestReservations(tokenStorage)
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
	importer.ImportNonPartitioned(resolver, localNode.DataLocation)
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
