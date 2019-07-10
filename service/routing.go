package service

import (
	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxql"
	"log"
	"net/http"
)

type RoutingFunc func(w http.ResponseWriter, r *http.Request, stmt influxql.Statement) ([]Result, error)

func RouteToAll(resolver *cluster.Resolver, client *http.Client) RoutingFunc {
	return func(w http.ResponseWriter, r *http.Request, stmt influxql.Statement) ([]Result, error) {
		// TODO In case one request fails, store in a log in peristent storage so that the command can be replayed in order
		// If it is a delete request, it is important that it is applied before recovering new writes.
		// All these requests which are mutable, should be added to a log that all nodes schould read from the first thing they do on startup after the local influxdb process is reachable.
		var allResults []Result

		for _, location := range resolver.FindAll() {
			if !cluster.IsAlive(location, client) {
				err := fmt.Errorf("All nodes must be available for statement \"%s\". Could not connect to: %s", stmt.String(), location)
				jsonError(w, http.StatusBadRequest, err.Error())
				return allResults, err
			}
		}

		for _, location := range resolver.FindAll() {
			results, err, res := request(stmt.String(), location, client, r)
			if err != nil {
				// We may want to handle errors differently
				// Eg. with a retry,
				passBack(w, res)
				return allResults, nil
			}
			// Maybe only send one of the results.
			allResults = append(allResults, results...)
		}
		return allResults, nil
	}
}

func RouteToFirstAvailable(resolver *cluster.Resolver, client *http.Client) RoutingFunc {
	return func(w http.ResponseWriter, r *http.Request, stmt influxql.Statement) ([]Result, error) {
		all := resolver.FindAll()
		for ri, location := range all {
			// Try requesting every single replica. Only if last one fails
			// return an error.
			results, err, res := request(stmt.String(), location, client, r)
			if ri == len(all)-1 && err != nil {
				passBack(w, res)
				return nil, fmt.Errorf("bad request")
			}
			return results, nil
		}
		return []Result{}, nil
	}
}

func RouteWithCoordination(resolver *cluster.Resolver, partitioner cluster.Partitioner, db string) RoutingFunc {
	return func(w http.ResponseWriter, r *http.Request, stmt influxql.Statement) ([]Result, error) {
		c := &Coordinator{resolver, partitioner}
		results, err, res := c.Handle(stmt.(*influxql.SelectStatement), r, db)
		if err != nil {
			log.Println(err)
			if res != nil {
				passBack(w, res)
			} else {
				jsonError(w, http.StatusBadRequest, err.Error())
				return nil, fmt.Errorf("bad request")
			}
		}
		// TODO Add support for chunked results, in which case we should stream data in chunks
		return results, nil
	}
}

func RouteAuthService(authService AuthService) RoutingFunc {
	return func(w http.ResponseWriter, r *http.Request, stmt influxql.Statement) ([]Result, error) {
		if results, err := HandleAuthStatement(stmt, authService); err != nil {
			handleBadRequestError(w, err)
			return nil, fmt.Errorf("bad request")
		} else {
			return results, nil
		}
	}
}

type RoutingStrategyFactory struct {
	resolver    *cluster.Resolver
	partitioner cluster.Partitioner
	authService AuthService
	client      *http.Client
}

func (rsf *RoutingStrategyFactory) Build(stmt influxql.Statement, db string) RoutingFunc {
	switch stmt.(type) {
	case *influxql.CreateContinuousQueryStatement,
		*influxql.CreateDatabaseStatement,
		*influxql.CreateRetentionPolicyStatement,
		*influxql.CreateSubscriptionStatement,
		*influxql.DropContinuousQueryStatement,
		*influxql.DropDatabaseStatement,
		*influxql.DropMeasurementStatement,
		*influxql.DropRetentionPolicyStatement,
		*influxql.DropSubscriptionStatement, // Need to figure out how to handle subscriptions with replication
		// Deletes could be multi-casted
		*influxql.DeleteSeriesStatement,
		*influxql.DeleteStatement,
		*influxql.DropSeriesStatement:
		return RouteToAll(rsf.resolver, rsf.client)

	case *influxql.DropShardStatement,
		*influxql.KillQueryStatement,
		*influxql.ShowShardGroupsStatement,
		*influxql.ShowShardsStatement,
		*influxql.ShowStatsStatement,
		*influxql.ShowDiagnosticsStatement:
		return nil

	case *influxql.ShowContinuousQueriesStatement,
		*influxql.ShowGrantsForUserStatement,
		*influxql.ShowDatabasesStatement,
		*influxql.ShowFieldKeysStatement,
		*influxql.ShowRetentionPoliciesStatement,
		*influxql.ShowSubscriptionsStatement,
		*influxql.ShowTagKeysStatement:
		return RouteToFirstAvailable(rsf.resolver, rsf.client)

	case *influxql.ShowMeasurementsStatement,
		*influxql.ShowSeriesStatement,
		*influxql.ShowTagValuesStatement,
		*influxql.ShowQueriesStatement:
		// TODO implement merging of results
		return RouteToFirstAvailable(rsf.resolver, rsf.client)

	case *influxql.SelectStatement:
		return RouteWithCoordination(rsf.resolver, rsf.partitioner, db)

	case *influxql.CreateUserStatement,
		*influxql.DropUserStatement,
		*influxql.GrantStatement,
		*influxql.GrantAdminStatement,
		*influxql.RevokeStatement,
		*influxql.RevokeAdminStatement,
		*influxql.SetPasswordUserStatement,
		*influxql.ShowUsersStatement:
		return RouteAuthService(rsf.authService)
	}

	return nil
}
