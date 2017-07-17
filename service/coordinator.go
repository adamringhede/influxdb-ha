package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb/influxql"
	"math/rand"
	"net/http"
	"time"
	"log"
	"github.com/davecgh/go-spew/spew"
)

// Coordinator handles a SELECT query using partition keys and a resolver.
// A query is decomposed in order to create a SelectStrategy based on
// the select statement and sources. Conditions are also parsed
// to determine if they fulfill the partition key for each measurement and to hash  tag values
// in order to resolve the correct nodes. It then executes the strategy
// by sending transformed queries to the resolved nodes and aggregating
// the results.
type Coordinator struct {
	resolver *cluster.Resolver
	//partitionKeys map[string]cluster.PartitionKey
	partitioner *cluster.Partitioner
}

func (c *Coordinator) Handle(stmt *influxql.SelectStatement, r *http.Request, db string) ([]result, error, *http.Response) {
	client := &http.Client{Timeout: 10 * time.Second}

	measurements := findMeasurements(stmt.Sources)
	// First assumption: one measurement, one query
	msmt := measurements[0]
	if msmt.Database == "" {
		msmt.Database = db
	}
	var locations []string
	pKey, ok := c.partitioner.GetKeyByMeasurement(msmt.Database, msmt.Name)
	if ok {
		//return []result{}, fmt.Errorf("The measurement %s - %s does not have a partition key", msmt.Database, msmt.Name), nil
		hashes := c.partitioner.GetHashes(pKey, getTagValues(stmt))
		/*
		TODO
		- If the key is not fulfilled, n / rf nodes need to be queried as the data is partitioned and the query will need to reach all servers.
		 */

		allResults, err := performQuery(stmt, r, hashes, c.resolver, client)
		if err != nil {
			return []result{}, err, nil
		}
		spew.Dump(allResults)
		// Merge allResults


		//locations = c.resolver.FindByKey(numericHash, cluster.READ)
		//log.Printf("Sharding found locations %s", strings.Join(locations, ", "))
	} else {
		log.Printf("[Coordinator] Measurement %s - %s does not have a partition key. Selecting any node.", msmt.Database, msmt.Name)
		locations = c.resolver.FindAll()
		selectedLocation := locations[rand.Intn(len(locations))]
		log.Printf("Selecting location %s", selectedLocation)
		return request(stmt, selectedLocation, client, r)
	}
	return []result{}, nil, nil
}

func performQuery(stmt *influxql.SelectStatement, r *http.Request, hashes []int, resolver *cluster.Resolver, client *http.Client) ([][]result, error) {
	locationsAsked := map[string]bool{}
	// TODO replace allResults with a channel.
	allResults := [][]result{}
	for _, hash := range hashes {
		// If none of the locations for a certain hash can respond, then no result
		// should be returned as it would be partial. This could be configured with an allowPartialResponses parameter.
		var err error
		for _, location := range resolver.FindByKey(hash, cluster.READ) {
			if !locationsAsked[location] {
				results, qErr, _ := request(stmt, location, client, r)
				if qErr != nil {
					err = qErr
				} else {
					locationsAsked[location] = true
					allResults = append(allResults, results)
					err = nil
					break
				}
			} else {
				break
			}
		}
		if err != nil {
			return allResults, err
		}
	}
	return allResults, nil
}

func mergeResults(results []result, stmt *influxql.SelectStatement) result {
	/*

	SPREAD needs to be decomposed into MAX and MIN, and then merged.

	MODE should behave like normal
	MEDIAN and MEAN should both use an arithmetic mean

	If the stmt is not performing grouping, aggregation or selection, it is
	fine to just concatenate values and sort if necessary.

	If there is only a single result, then just return it.
	 */
	if len(stmt.Dimensions) == 0 {
		// This obviously does not work if on of the results is empty.
		// And it needs to work with any amount of results as well
		results[0].Series[0].Values = append(results[0].Series[0].Values, results[1].Series[0].Values...)
	} else {
		//
	}
	return results[0]
}

type valueConfig struct {
	aggregation string
}

type valueResult struct {
}

type Strategy struct {
	database    string
	measurement string
	hashedKey   int
	values      map[string]valueConfig
}

// Executor executes a strategy in parallel and then maybe merges the results or just return the series for each key.
type Executor struct {
}

type tagFinder struct {
	tags   map[string]bool
	values map[string][]string // it could be interface to allow for numerical values as well
}

func newTagFinder() *tagFinder {
	return &tagFinder{make(map[string]bool), make(map[string][]string)}
}

func getTagValues(stmt *influxql.SelectStatement) map[string][]string {
	finder := newTagFinder()
	for _, condition := range findConditions(stmt) {
		finder.findTags(condition)
	}
	return finder.values
}

func (f *tagFinder) putValue(key string, value interface{}) {
	_, ok := f.values[key]
	if !ok {
		f.values[key] = []string{}
	}
	switch v := value.(type) {
	case *influxql.VarRef:
		f.values[key] = append(f.values[key], v.Val)
	case *influxql.StringLiteral:
		f.values[key] = append(f.values[key], v.Val)
	default:
		panic("Could not find a place for " + key)
	}

}

func (f *tagFinder) findTags(cond influxql.Expr) {
	// if op isn't 22 or 23, then it is some equality operation.
	switch expr := cond.(type) {
	case *influxql.ParenExpr:
		f.findTags(expr.Expr)
	case *influxql.BinaryExpr:
		if expr.Op == influxql.AND || expr.Op == influxql.OR {
			f.findTags(expr.LHS)
			f.findTags(expr.RHS)
		} else {
			tagKey := expr.LHS.(*influxql.VarRef).Val
			// Greater than and less than operators are not supported.
			// Instead, the application should define buckets to evenly
			// distribute tags values.
			if expr.Op == influxql.EQ {
				current, ok := f.tags[tagKey]
				if !ok || current != false {
					f.tags[tagKey] = true
					f.putValue(tagKey, expr.RHS)
				}
			} else {
				f.tags[tagKey] = false
			}
		}
	}
}

func findConditions(stmt *influxql.SelectStatement) []influxql.Expr {
	conditions := []influxql.Expr{}
	conditions = append(conditions, stmt.Condition)
	for _, source := range stmt.Sources {
		switch s := source.(type) {
		case *influxql.SubQuery:
			conditions = append(conditions, findConditions(s.Statement)...)
		}
	}
	return conditions
}

func findMeasurements(srcs []influxql.Source) []*influxql.Measurement {
	measurements := []*influxql.Measurement{}
	for _, source := range srcs {
		switch s := source.(type) {
		case *influxql.SubQuery:
			measurements = append(measurements, findMeasurements(s.Statement.Sources)...)
		case *influxql.Measurement:
			measurements = append(measurements, s)
		}
	}
	return measurements
}
