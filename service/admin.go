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
	authService			AuthService
}

func (h *ClusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queryParam := r.URL.Query().Get("q")
	if !h.checkAccess(w, r) { return }

	stmt, err := clusterql.NewParser(strings.NewReader(queryParam), clusterLanguage).Parse()
	if err != nil {
		jsonError(w, http.StatusBadRequest, "error parsing query: "+err.Error())
		return
	}

	switch _stmt := stmt.(type) {
	case clusterql.ShowPartitionKeysStatement:
		handleShowPartitionKeys(_stmt, h.partitionKeyStorage, w)
	case clusterql.CreatePartitionKeyStatement:
		handleCreatePartitionKey(_stmt, h.partitionKeyStorage, w)
	case clusterql.DropPartitionKeyStatement:
		handleDropPartitionKey(_stmt, h.partitionKeyStorage, w)
	case clusterql.RemoveNodeStatement:
		handleRemoveNode(_stmt, h.nodeStorage, w)
	case clusterql.ShowNodesStatement:
		handleShowNodes(_stmt, h.nodeStorage, w)
	default:
		jsonError(w, http.StatusInternalServerError, "not implemented")
	}
}

func (h *ClusterHandler) checkAccess(w http.ResponseWriter, r *http.Request) bool {
	if h.authService != nil {
		user, err := authenticate(r, h.authService)
		if err != nil {
			handleErrorWithCode(w, err, http.StatusUnauthorized)
			return false
		}

		if !user.AuthorizeClusterOperation() {
			jsonError(w, http.StatusForbidden, "forbidden cluster statement")
			return false
		}
	}
	return true
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

func handleShowPartitionKeys(stmt clusterql.ShowPartitionKeysStatement, pks cluster.PartitionKeyStorage, w http.ResponseWriter) {
	keys, err := pks.GetAll()
	handleInternalError(w, err)
	columns := []string{"database", "measurement", "tags"}
	var values [][]interface{}
	for _, key := range keys {
		if stmt.Database == "" || stmt.Database == key.Database {
			values = append(values, []interface{}{key.Database, key.Measurement, strings.Join(key.Tags, ".")})
		}
	}
	respondWithResults(w, createListResults("partition keys", columns, values))
}

func handleCreatePartitionKey(stmt clusterql.CreatePartitionKeyStatement, pks cluster.PartitionKeyStorage, w http.ResponseWriter) {
	partitionKey := &cluster.PartitionKey{Database: stmt.Database, Measurement: stmt.Measurement, Tags: stmt.Tags}
	// check that one not already exists
	// create and save one
	keys, err := pks.GetAll()
	handleInternalError(w, err)
	for _, pk := range keys {
		if pk.Identifier() == partitionKey.Identifier() {
			jsonError(w, http.StatusConflict, "a partition key already exist on "+pk.Identifier())
			return
		}
	}
	// It should not be possible to create a partition token for a collection that already has one.
	saveErr := pks.Save(partitionKey)
	handleInternalError(w, saveErr)
	respondWithEmpty(w)
}

func handleDropPartitionKey(stmt clusterql.DropPartitionKeyStatement, pks cluster.PartitionKeyStorage, w http.ResponseWriter) {
	err := pks.Drop(stmt.Database, stmt.Measurement)
	handleInternalError(w, err)
	respondWithEmpty(w)
}

func handleRemoveNode(stmt clusterql.RemoveNodeStatement, storage cluster.NodeStorage, w http.ResponseWriter) {
	name := stmt.Name
	ok, err := storage.Remove(name)
	// TODO Admins need a way of monitoring the state of imports happening.
	handleInternalError(w, err)
	if !ok {
		jsonError(w, http.StatusNotFound, "could not find node with name \""+name+"\"")
		return
	}
	respondWithEmpty(w)
}

func handleShowNodes(stmt clusterql.ShowNodesStatement, storage cluster.NodeStorage, w http.ResponseWriter) {
	values := [][]interface{}{}
	nodes, err := storage.GetAll()
	handleInternalError(w, err)
	for _, node := range nodes {
		values = append(values, []interface{}{node.Name, node.DataLocation})
	}
	respondWithResults(w, createListResults("nodes", []string{"name", "data location"}, values))
}
