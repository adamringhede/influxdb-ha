package service

import (
	"fmt"
	"log"
		"net/http"
	"strings"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/service/merge"
	"github.com/influxdata/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/adamringhede/influxdb-ha/hash"
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
	partitioner cluster.Partitioner
}

type compareLess func(a, b interface{}) bool

func lessRfc(a, b interface{}) bool {
	ats, err := time.Parse(time.RFC3339Nano, a.(string))
	if err != nil {
		log.Panic(err)
	}
	bts, err := time.Parse(time.RFC3339Nano, b.(string))
	if err != nil {
		log.Panic(err)
	}
	return ats.UnixNano() < bts.UnixNano()
}

func lessFloat(a, b interface{}) bool {
	return a.(float64) < b.(float64)
}

func groupResultsByTags(allResults [][]Result) map[string][]Result {
	groupedResults := map[string][]Result{}
	// Group allResults by combination of tags
	for _, results := range allResults {
		for _, res := range results {
			tagsKey := ""
			if len(res.Series) > 0 {
				for key, value := range res.Series[0].Tags {
					tagsKey += key + "=" + value + ","
				}
			}
			if _, ok := groupedResults[tagsKey]; !ok {
				groupedResults[tagsKey] = []Result{}
			}
			groupedResults[tagsKey] = append(groupedResults[tagsKey], res)
		}
	}
	return groupedResults
}

func timeGreaterThan(a, b string) bool {
	ats, err := time.Parse(time.RFC3339Nano, a)
	if err != nil {
		log.Panic(err)
	}
	bts, err := time.Parse(time.RFC3339Nano, b)
	if err != nil {
		log.Panic(err)
	}
	return ats.Nanosecond() > bts.Nanosecond()
}

// mergeSortResults assuems that there is only one series
func mergeSortResults(groupedResults map[string][]Result, less compareLess) []Result {
	mergedResults := []Result{}
	for _, group := range groupedResults {
		merged := Result{}
		merged.Series = []*models.Row{{
			Columns: []string{},        // TODO make sure to set column and that they are in the same order from the results
			Values:  [][]interface{}{}, // TODO preallocate memory for the values
		}}
		allDone := false
		for !allDone {
			min := 0
			allDone = true
			for i, result := range group {
				if len(result.Series[0].Values) > 0 {
					allDone = false
				} else {
					continue
				}
				if len(group[min].Series[0].Values) == 0 || less(result.Series[0].Values[0][0], group[min].Series[0].Values[0][0]) {
					min = i
				}
			}
			if len(group[min].Series[0].Values) > 0 {
				merged.Series[0].Values = append(merged.Series[0].Values, group[min].Series[0].Values[0])
				group[min].Series[0].Values = group[min].Series[0].Values[1:]
			}
		}
		mergedResults = append(mergedResults, merged)

	}
	return mergedResults
}

func mergeQueryResults(groupedResults map[string][]Result, tree *merge.QueryTree) []Result {
	mergedResults := []Result{}
	for _, group := range groupedResults {
		src := NewResultSource(group)
		merged := Result{}
		merged.Series = []*models.Row{{
			Columns: []string{"time"}, // TODO make sure to add the columns from the results and that they are in the same order
			Values:  [][]interface{}{},
		}}
		for src.Reset(); !src.Done(); src.Step() {
			for _, f := range tree.Fields {
				merged.Series[0].Columns = append(merged.Series[0].Columns, f.ResponseField)
			}

			value := []interface{}{src.Time()}
			for _, f := range tree.Fields {
				switch f.Root.(type) {
				case *merge.Top, *merge.Bottom:
					// TODO Add support for adding multiple values. Note that in 1.3 is is no longe possible to use
					// both aggregations function and top/bottom which makes this easier.
				}
				value = append(value, f.Root.Next(src)[0])
			}
			merged.Series[0].Values = append(merged.Series[0].Values, value)
		}
		mergedResults = append(mergedResults, merged)
	}
	return mergedResults
}

func hasCall(stmt *influxql.SelectStatement) bool {
	for _, field := range stmt.Fields {
		if _, ok := field.Expr.(*influxql.Call); ok {
			return true
		}
	}
	return false
}

func (c *Coordinator) Handle(stmt *influxql.SelectStatement, r *http.Request, db string) ([]Result, error, *http.Response) {
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
		tagValues := getTagValues(stmt)
		if !c.partitioner.FulfillsKey(pKey, tagValues) {
			/*
				TODO
				If the key is not fulfilled, ideally n / rf nodes need to be queried as the data
				is partitioned and the query will need to reach all servers.
				However, finding those as well as the replicas if one fails is trickier.
			*/
			return []Result{}, fmt.Errorf("the partition key is not fulfilled given the tags in the query"), nil
		}
		hashes := c.partitioner.GetHashes(pKey, tagValues)

		if len(hashes) > 1 {
			interval, err := stmt.GroupByInterval()
			if err != nil {
				return []Result{}, err, nil
			}
			// A selection with a call need to be handled differently as results from calls from different
			// nodes need to be merged. However if there is no aggregation, it is enough to just merge
			// the result and maintain sort.
			if interval == 0 && !hasCall(stmt) {
				allResults, err, response := performQuery(stmt.String(), r, hashes, c.resolver, client)
				if err != nil {
					return []Result{}, err, nil
				}
				groupedResults := groupResultsByTags(allResults)
				precision := r.URL.Query().Get("precision")
				var less compareLess
				if precision == "" || strings.ToUpper(precision) == "RFC3339" {
					less = lessRfc
				} else {
					less = lessFloat
				}
				mergedResults := mergeSortResults(groupedResults, less)
				return mergedResults, nil, response
			} else {
				// Divide the query and merge the results
				tree, qb, err := merge.NewQueryTree(stmt)
				if err != nil {
					return []Result{}, err, nil
				}
				s := qb.CreateStatement(stmt)

				allResults, err, response := performQuery(s, r, hashes, c.resolver, client)
				if err != nil {
					return []Result{}, err, nil
				}

				groupedResults := groupResultsByTags(allResults)
				mergedResults := mergeQueryResults(groupedResults, tree)
				return mergedResults, nil, response
			}
		} else {
			// Request single nodes
			locations = c.resolver.FindByKey(hashes[0], cluster.READ)
			return requestMultipleLocations(stmt.String(), locations, client, r)
		}
	} else {
		// Resolve based on database name
		key := hash.String(cluster.CreatePartitionKeyIdentifier(db, ""))
		locations := c.resolver.FindByKey(int(key), cluster.READ)
		return requestMultipleLocations(stmt.String(), locations, client, r)
	}
	return []Result{}, nil, nil
}

func requestMultipleLocations(stmt string, locations []string, client *http.Client, r *http.Request) ([]Result, error, *http.Response) {
	for _, location := range locations {
		results, err, res := request(stmt, location, client, r)
		if err == nil {
			return results, err, res
		}
	}
	return []Result{}, nil, nil
}

func performQuery(stmt string, r *http.Request, hashes []int, resolver *cluster.Resolver, client *http.Client) ([][]Result, error, *http.Response) {
	locationsAsked := map[string]bool{}
	// TODO replace allResults with a channel and make requests in parallel.
	allResults := [][]Result{}
	var response *http.Response
hashLoop:
	for _, hash := range hashes {
		// If none of the locations for a certain hash can respond, then no Result
		// should be returned as it would be partial. This could be configured with an allowPartialResponses parameter.
		var err error
		locations := resolver.FindByKey(hash, cluster.READ)
		for _, location := range locations {
			// See if any node with data for this has has already been requested. If so, then this hash can be skipped.
			if locationsAsked[location] {
				continue hashLoop
			}
		}
		// TODO Improve load balancing of nodes. Eg. prioritize based on certain attributes.
		for _, location := range locations {
			if !locationsAsked[location] {
				results, qErr, res := request(stmt, location, client, r)
				response = res
				if qErr != nil {
					err = qErr
				} else {
					locationsAsked[location] = true
					allResults = append(allResults, results)
					err = nil
					break
				}
			} else {
				// If one node that has data for this hash has already been requested,
				// then there is no point in trying any location, which is the reason for breaking.
				break
			}
		}
		if err != nil {
			return allResults, err, response
		}
	}
	return allResults, nil, response
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

type ResultSource struct {
	data         []resultGroup
	fieldIndices map[string]int
	i            int
}

type resultGroup struct {
	time   string
	values []interface{}
}

func NewResultSource(results []Result) *ResultSource {
	source := &ResultSource{
		data:         []resultGroup{},
		fieldIndices: map[string]int{},
	}

	// Group values by time and ensure order. This assumes that both results have the exact same time-steps and
	// that times are strings.
	a := map[string]int{}
	ai := 0
	for _, res := range results {
		for _, series := range res.Series {
			for _, v := range series.Values {
				k := string(v[0].(string))
				if _, ok := a[k]; !ok {
					source.data = append(source.data, resultGroup{k, []interface{}{}})
					a[k] = ai
					ai += 1
				}
				group := source.data[a[k]]
				source.data[a[k]].values = append(group.values, v)
			}
		}
	}
	// TODO handle empty result
	for i, col := range results[0].Series[0].Columns {
		source.fieldIndices[col] = i
	}

	return source
}

func (s *ResultSource) Next(fieldKey string) []float64 {
	res := make([]float64, len(s.data[s.i].values))
	fieldIndex, fieldExists := s.fieldIndices[fieldKey]
	if !fieldExists {
		panic(fmt.Errorf("No values exist for field key %s", fieldKey))
	}
	if s.Done() {
		return res
	}
	for i, v := range s.data[s.i].values {
		if data, ok := v.([]interface{}); ok {
			switch value := data[fieldIndex].(type) {
			case int:
				res[i] = float64(value)
			case float64:
				res[i] = value
			case nil:
				res[i] = 0
			default:
				panic(fmt.Errorf("Unsupported type %T", value))
			}

		}
	}
	return res
}

func (s *ResultSource) Time() string {
	if s.Done() {
		return ""
	}
	return s.data[s.i].time
}

func (s *ResultSource) Step() {
	s.i += 1
}

func (s *ResultSource) Done() bool {
	return s.i >= len(s.data)
}

func (s *ResultSource) Reset() {
	s.i = 0
}
