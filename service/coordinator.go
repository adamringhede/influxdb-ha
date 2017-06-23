package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb/influxql"
	"math/rand"
	"net/http"
	"time"
	"log"
	"strings"
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
	// assuming that the query is only for one measurement and that all tag values are strings
	// test if the query can be used with the measurement's partition key
	// 	- at least one value has to be equal to each tag key
	// create hashed values from the sorted combination of tags matching the key
	//	- for multiple equal operators, one hash has to be created for each

	// assuming that the query is a mean aggregation on one or more value
	// create a strategy with the keys and information about each value to return and the source measurement

	// merge the results using the strategy keys and aggregation method.

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
		numericHash, hashErr := c.partitioner.GetHash(pKey, getTagValues(stmt))
		if hashErr != nil {
			return []result{}, hashErr, nil
		}
		locations = c.resolver.FindByKey(numericHash, cluster.READ)
		log.Printf("Sharding found locations %s", strings.Join(locations, ", "))
	} else {
		log.Printf("[Coordinator] Broadacasting. Measurement %s - %s does not have a partition key.", msmt.Database, msmt.Name)
		locations = c.resolver.FindAll()
	}

	// sending the query to only a single location does not work when broadcasting
	// all servers / number of replicas have to be queried and results need to be merged.
	selectedLocation := locations[rand.Intn(len(locations))]
	log.Printf("Selecting location %s", selectedLocation)
	client := &http.Client{Timeout: 10 * time.Second}
	results, err, res := request(stmt, selectedLocation, client, r)
	return results, err, res
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
