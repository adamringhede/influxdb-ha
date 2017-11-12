package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/service/clusterql"
	"github.com/influxdata/influxdb/models"
	"net/http"
	"regexp"
	"strings"
)

var clusterLanguage = clusterql.CreateLanguage()

func isAdminQuery(queryParam string) bool {
	matched, err := regexp.MatchString("(SHOW|DROP|CREATE|SET)\\s+(PARTITION|REPLICATION)", strings.ToUpper(queryParam))
	if err != nil {
		panic(err)
	}
	return matched
}

type ClusterHandler struct {
	partitionKeyStorage cluster.PartitionKeyStorage
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
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
		}
		columns := []string{"database", "measurement", "tags"}
		values := [][]interface{}{}
		for _, key := range keys {
			values = append(values, []interface{}{key.Database, key.Measurement, strings.Join(key.Tags, ".")})
		}
		respondWithResults(&w, createListResults("partition keys", columns, values))
		return
	case clusterql.DropPartitionKeyStatement:
		err := h.partitionKeyStorage.Drop(
			stmt.(clusterql.DropPartitionKeyStatement).Database,
			stmt.(clusterql.DropPartitionKeyStatement).Measurement,
		)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, err.Error())
		}
		respondWithEmpty(&w)
		return
	}
}

func createListResults(name string, columns []string, values [][]interface{}) []Result {
	return []Result{{Series: []*models.Row{{
		Name:    name,
		Columns: columns,
		Values:  values,
	}}}}
}
