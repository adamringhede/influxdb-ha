package service

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/service/clusterql"
	"github.com/influxdata/influxdb/models"
	"fmt"
)

var clusterLanguage = clusterql.CreateLanguage()

func isAdminQuery(queryParam string) bool {
	matched, err := regexp.MatchString("(REMOVE|SHOW|DROP|CREATE|SET)\\s+(NODES|NODE|PARTITION|REPLICATION)", strings.ToUpper(queryParam))
	if err != nil {
		fmt.Printf("Warning: Rexexp error on query: %s\n", err.Error())
	}
	return matched
}

type ClusterHandler struct {
	partitionKeyStorage cluster.PartitionKeyStorage
	nodeStorage         cluster.NodeStorage
}

func (h *ClusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queryParam := r.URL.Query().Get("q")

	// TODO add support for multiple statements in single query
	// TODO support using both influxdb statements and cluster statements in same query

	stmt, err := clusterql.NewParser(strings.NewReader(queryParam), clusterLanguage).Parse()
	if err != nil {
		jsonError(w, http.StatusBadRequest, "error parsing query: "+err.Error())
		return
	}
	switch stmt.(type) {
	case clusterql.ShowPartitionKeysStatement:
		keys, err := h.partitionKeyStorage.GetAll()
		handleInternalError(w, err)
		dbFilter := stmt.(clusterql.ShowPartitionKeysStatement).Database
		columns := []string{"database", "measurement", "tags"}
		values := [][]interface{}{}
		for _, key := range keys {
			if dbFilter == "" || dbFilter == key.Database {
				values = append(values, []interface{}{key.Database, key.Measurement, strings.Join(key.Tags, ".")})
			}
		}
		respondWithResults(w, createListResults("partition keys", columns, values))
		return
	case clusterql.CreatePartitionKeyStatement:
		input := stmt.(clusterql.CreatePartitionKeyStatement)
		partitionKey := &cluster.PartitionKey{Database: input.Database, Measurement: input.Measurement, Tags: input.Tags}
		// check that one not already exists
		// create and save one
		keys, err := h.partitionKeyStorage.GetAll()
		handleInternalError(w, err)
		for _, pk := range keys {
			if pk.Identifier() == partitionKey.Identifier() {
				jsonError(w, http.StatusConflict, "a partition key already exist on "+pk.Identifier())
				return
			}
		}
		// It should not be possible to create a partition token for a collection that already has one.
		saveErr := h.partitionKeyStorage.Save(partitionKey)
		handleInternalError(w, saveErr)
		respondWithEmpty(w)
	case clusterql.DropPartitionKeyStatement:
		err := h.partitionKeyStorage.Drop(
			stmt.(clusterql.DropPartitionKeyStatement).Database,
			stmt.(clusterql.DropPartitionKeyStatement).Measurement,
		)
		handleInternalError(w, err)
		respondWithEmpty(w)
		return
	case clusterql.RemoveNodeStatement:
		name := stmt.(clusterql.RemoveNodeStatement).Name
		ok, err := h.nodeStorage.Remove(name)
		// TODO distribute tokens to other clients and have them start importing data.
		handleInternalError(w, err)
		if !ok {
			jsonError(w, http.StatusNotFound, "could not find node with name \""+name+"\"")
			return
		}
		respondWithEmpty(w)
		return
	case clusterql.ShowNodesStatement:
		values := [][]interface{}{}
		nodes, err := h.nodeStorage.GetAll()
		handleInternalError(w, err)
		for _, node := range nodes {
			values = append(values, []interface{}{node.Name, node.DataLocation})
		}
		respondWithResults(w, createListResults("nodes", []string{"name", "data location"}, values))
		return
	default:
		jsonError(w, http.StatusInternalServerError, "not implemented")
	}
}

func handleInternalError(w http.ResponseWriter, err error) {
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
	}
}

func createListResults(name string, columns []string, values [][]interface{}) []Result {
	return []Result{{Series: []*models.Row{{
		Name:    name,
		Columns: columns,
		Values:  values,
	}}}}
}
