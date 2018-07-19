package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxql"
	"net/http"
	"time"
)

const defaultDB = "default"

type QueryHandler struct {
	client         *http.Client
	resolver       *cluster.Resolver
	partitioner    cluster.Partitioner
	clusterHandler *ClusterHandler
	authService    AuthService
	routeFactory   *RoutingStrategyFactory
}

func NewQueryHandler(resolver *cluster.Resolver, partitioner cluster.Partitioner,
	clusterHandler *ClusterHandler, authService AuthService) *QueryHandler {

	client := &http.Client{Timeout: 10 * time.Second}
	routeFactory := &RoutingStrategyFactory{resolver, partitioner, authService, client}

	return &QueryHandler{client, resolver, partitioner,
		clusterHandler, authService, routeFactory}
}

func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queryParam := r.URL.Query().Get("q")
	if queryParam == "" {
		jsonError(w, http.StatusBadRequest, "missing required parameter \"q\"")
		return
	}

	if isAdminQuery(queryParam) {
		h.clusterHandler.ServeHTTP(w, r)
		return
	}

	q, err := influxql.ParseQuery(queryParam)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "error parsing query: "+err.Error())
		return
	}

	db := r.URL.Query().Get("db")
	if db == "" {
		db = defaultDB
	}

	if !h.checkAccess(w, r, q, db) {
		return
	}

	var allResults []Result

	for _, stmt := range q.Statements {
		if route := h.routeFactory.Build(stmt, db); route != nil {
			results := route(w, r, stmt)
			allResults = append(allResults, results...)
		} else {
			// Not supported. Client must connect to the individual data node.
			jsonError(w, 400, "Statement is not supported on cluster: "+stmt.String())
			return
		}
	}

	if _, ok := h.authService.(*PersistentAuthService); ok {
		// There is a danger here. If we use another implementation of the auth service, no changes will be persisted
		err := h.authService.(*PersistentAuthService).Save()
		if err != nil {
			handleInternalError(w, err)
			return
		}
	}

	// TODO replace with a dynamic ResultFlusher that can either save all
	// results in memory and flushes in the end, or flushes every chunk received.
	respondWithResults(w, allResults)

}

func (h *QueryHandler) checkAccess(w http.ResponseWriter, r *http.Request, q *influxql.Query, db string) bool {
	if h.authService != nil {
		user, err := authenticate(r, h.authService)
		if err != nil {
			handleErrorWithCode(w, err, http.StatusUnauthorized)
			return false
		}

		// Check privileges for all statements first.
		for _, stmt := range q.Statements {
			if !h.authService.HasAdmin() {
				if s, firstUser := stmt.(*influxql.CreateUserStatement); firstUser && s.Admin {
					// If the first statement creates the first admin user, we skip checking privileges.
					break
				}
			}
			privileges, _ := stmt.RequiredPrivileges()
			if user == nil || !isAllowed(privileges, *user, db) {
				jsonError(w, http.StatusForbidden, "forbidden statement: "+stmt.String())
				return false
			}
		}
	}
	return true
}
