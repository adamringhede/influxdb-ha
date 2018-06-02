package syncing

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/hash"
	"github.com/influxdata/influxdb/models"
)

type Loader interface {
	get(q string, location, db, rp string, chunked bool)
}

// TODO implement a test loader that just returns json

type ImportBatch struct {
	// index can be used as an offset to continue importing from it.
	index int
}

type Importer interface {
	Import(tokens []int, resolver *cluster.Resolver, target string)
	ImportNonPartitioned(resolver *cluster.Resolver, target string)
}

type ErrorNoNodeFound struct{ Token int }

func (err ErrorNoNodeFound) Error() string {
	return fmt.Sprintf("No node could be found for token %d", err.Token)
}

type ImportDecision int

const (
	NoImport ImportDecision = iota
	PartitionImport
	FullImport
)

// ImportPredicate isolates the logic for determining if data should be imported and makes it possible
// to later change the behaviour of the import
type ImportDecisionTester func(db, msmt string) ImportDecision

func AlwaysFullImport(db, msmt string) ImportDecision {
	return FullImport
}

func AlwaysNoImport(db, msmt string) ImportDecision {
	return NoImport
}

func AlwaysPartitionImport(db, msmt string) ImportDecision {
	return PartitionImport
}

type ClusterImportPredicate struct {
	LocalNode     cluster.Node
	PartitionKeys cluster.PartitionKeyCollection
	Resolver      *cluster.Resolver
}

// Test returns true if data should be imported for a given database and measurement.
// - test that the measurement has a partitionKey (otherwise it should not be imported by default)
// - test that the database hash resolves to the node given the configured replication factor
// The reason for hashing on database and not measurement is so that queries including multiple measurements
// don't need to be distributed.
func (p *ClusterImportPredicate) Test(db, msmt string) ImportDecision {
	for _, pk := range p.PartitionKeys.GetPartitionKeys() {
		if pk.Database == db && (pk.Measurement == msmt || pk.Measurement == "") {
			return PartitionImport
		}
	}
	key := hash.String(cluster.CreatePartitionKeyIdentifier(db, ""))
	nodes := p.Resolver.FindNodesByKey(int(key), cluster.WRITE)
	for _, node := range nodes {
		if node.Name == p.LocalNode.Name {
			return FullImport
		}
	}
	return NoImport
}

type BasicImporter struct {
	loader        Loader
	Predicate     ImportDecisionTester
	PartitionKeys cluster.PartitionKeyCollection
	Resolver      *cluster.Resolver

	// cache
	createdDatabases map[string]bool
	locationsMeta    map[string]locationMeta
}

func NewImporter(resolver *cluster.Resolver, partitionKeys cluster.PartitionKeyCollection, predicate ImportDecisionTester) *BasicImporter {
	return &BasicImporter{Predicate: predicate, Resolver: resolver, PartitionKeys: partitionKeys}
}

func (i *BasicImporter) ImportNonPartitioned(resolver *cluster.Resolver, target string) {
	for _, location := range resolver.FindAll() {
		i.forEachDatabase(location, target, func(db string, dbMeta *databaseMeta) {
			for _, msmt := range dbMeta.measurements {
				if importType := i.Predicate(db, msmt); importType == FullImport {
					for _, rp := range dbMeta.rps {
						importCh, _ := streamData(location, db, rp, msmt, "")
						persistStream(importCh, dbMeta, target, db, rp)
					}
				}
			}
		})
	}
}

// Import data given a set of tokens. The tokens should include those stolen from
// other nodes as well as token for which this node is holding replicated data
func (i *BasicImporter) Import(tokens []int, resolver *cluster.Resolver, target string) {
	for _, token := range tokens {
		nodes := resolver.FindByKey(token, cluster.READ)
		for _, location := range nodes {
			i.forEachDatabase(location, target, func(db string, dbMeta *databaseMeta) {
				i.importTokenData(location, target, token, db, dbMeta, resolver)
			})
		}
	}
	log.Println("Finished import")
}

func (i *BasicImporter) ensureCache() {
	if i.createdDatabases == nil {
		i.createdDatabases = map[string]bool{}
	}
	if i.locationsMeta == nil {
		i.locationsMeta = map[string]locationMeta{}
	}
}

func (i *BasicImporter) forEachDatabase(location string, target string, fn func(db string, dbMeta *databaseMeta)) {
	i.ensureCache()
	meta, ok := i.locationsMeta[location]
	if !ok {
		fetchedMeta, err := fetchLocationMeta(location)
		if err != nil {
			log.Printf("Failed fetching meta from location %s. Error: %s", location, err.Error())
			return
		}
		meta = fetchedMeta
		i.locationsMeta[location] = meta
	}
	for db, dbMeta := range meta.databases {
		if _, hasDB := i.createdDatabases[db]; !hasDB {
			createDatabase(db, target)
			createRPs(db, dbMeta.rpsSettings, target)

			i.createdDatabases[db] = true
		}
		fn(db, dbMeta)
	}
}

func (i *BasicImporter) importTokenData(location, target string, token int, db string, dbMeta *databaseMeta, resolver *cluster.Resolver) {
	for _, rp := range dbMeta.rps {
		log.Printf("Exporting data from database %s.%s", db, rp)

		// TODO Do not rely on partition tag for the import
		// Instead, get the partition keys for all measurements that exist on this node.
		// Then request all tag values for these partition keys. Create all possible combinations of these tags and find those that
		// would resolve to the token.
		// The query below shows how we can serch for series for the partition key
		for _, msmt := range dbMeta.measurements {
			if importType := i.Predicate(db, msmt); importType == PartitionImport {
				for _, series := range dbMeta.series {
					for _, pk := range i.PartitionKeys.GetPartitionKeys() {
						if pk.Measurement == msmt && series.Matches(token, pk, resolver) {
							importCh, _ := streamData(location, db, rp, msmt, series.Where())
							persistStream(importCh, dbMeta, target, db, rp)
						}
					}
				}
			}
		}
	}
}

func persistStream(importCh chan result, dbMeta *databaseMeta, target, db, rp string) {
	for res := range importCh {
		var lines []string
		for _, row := range res.Series {
			lines = append(lines, parseLines(row, dbMeta.tagKeys)...)
		}
		// TODO handle authentication
		_, err := postLines(target, db, rp, lines)
		if err != nil {
			// TODO handle any type of failure to write locally.
			// TODO find a better way to structure this to handle errors instead of panic.
			log.Panicf("Failed to post data to target node %s.", target)
		}
	}
}

func createDatabase(db, target string) {
	log.Printf("Creating database %s", db)
	handleInfluxError(get("CREATE DATABASE "+db, target, "", false))
}

func createRPs(db string, rps []RetentionPolicy, target string) {
	for _, rp := range rps {
		createRetentionPolicy(db, rp, target)
	}
}

func createRetentionPolicy(db string, rp RetentionPolicy, target string) {
	log.Printf("Creating retention policy %s", rp.Name)
	q := fmt.Sprintf(`CREATE RETENTION POLICY "%s" ON "%s" DURATION %s REPLICATION 1`, rp.Name, db, rp.Duration)
	if rp.Default {
		q += " DEFAULT"
	}
	handleInfluxError(get(q, target, db, false))
}

func handleInfluxError(resp *http.Response, err error) error {
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read body: %s", err)
		}
		log.Fatalf("Received invalid status code %d. Body: %s", resp.StatusCode, string(body))
	}
	return err
}


func postLines(location, db, rp string, lines []string) (*http.Response, error) {
	return postData(location, db, rp, []byte(strings.Join(lines, "\n")))
}

func postData(location, db, rp string, buf []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", "http://"+location+"/write", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	query := []string{
		"db=" + db,
		"rp=" + rp,
	}

	req.URL.RawQuery = strings.Join(query, "&")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	// TODO Fix authentication support. This node need admin access
	// This is different from user authentication.
	//if auth != "" {
	//	req.Header.Set("Authorization", auth)
	//}
	client := http.Client{Timeout: 60 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func parseLines(row *models.Row, tagKeys map[string]bool) []string {
	lines := []string{}
	for _, values := range row.Values {
		line := row.Name
		i := 1
		// Tags
		for ; i < len(row.Columns)-1; i += 1 {
			if _, ok := tagKeys[row.Name+"."+row.Columns[i]]; ok {
				if values[i] != nil {
					line += "," + row.Columns[i] + "=" + values[i].(string)
				}
			} else {
				break
			}
		}
		line += " "
		fieldValues := []string{}
		// Values
		for ; i < len(row.Columns); i += 1 {
			if values[i] != nil {
				fieldValues = append(fieldValues, row.Columns[i]+"="+convertToString(values[i]))
			}
		}
		line += strings.Join(fieldValues, ",")
		// Time
		timeStr := values[0].(string)
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			panic(err)
		}
		line += " " + strconv.Itoa(ts.Nanosecond())
		lines = append(lines, line)
	}
	return lines
}

func convertToString(value interface{}) string {
	switch value.(type) {
	case float64:
		return strconv.FormatFloat(value.(float64), 'f', 6, 64)
	case float32:
		return strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
	}
	return ""
}

func fetchLocationMeta(location string) (locationMeta, error) {
	log.Printf("Fetching meta data from %s", location)
	meta := newLocationMeta()
	databases, err := fetchDatabases(location)
	if err != nil {
		return meta, err
	}
	for _, db := range databases {
		if db == "_internal" {
			continue
		}
		// TODO figure out if it is needed to use pointers here
		meta.databases[db] = newDatabaseMeta()
		dbMeta := meta.databases[db]
		measurements, err := fetchMeasurements(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.measurements = measurements
		rps, err := fetchRetentionPolicies(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.rps = rps
		rpsSettings, err := fetchRetentionPoliciesWithSettings(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.rpsSettings = rpsSettings
		tagKeys, err := fetchTagKeys(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.tagKeys = tagKeys
		series, err := FetchSeries(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.series = series
	}
	return meta, nil
}

type locationMeta struct {
	databases map[string]*databaseMeta
}

func newLocationMeta() locationMeta {
	return locationMeta{map[string]*databaseMeta{}}
}

type databaseMeta struct {
	measurements []string
	rps          []string
	rpsSettings  []RetentionPolicy
	tagKeys      map[string]bool
	series       []Series
}

func newDatabaseMeta() *databaseMeta {
	return &databaseMeta{[]string{}, []string{}, []RetentionPolicy{},
		map[string]bool{}, []Series{}}
}

func get(q string, location string, db string, chunked bool) (*http.Response, error) {
	client := &http.Client{}
	params := []string{"db=" + db, "q=" + q, "chunked=" + strconv.FormatBool(chunked)}
	values, err := url.ParseQuery(strings.Join(params, "&"))
	if err != nil {
		log.Panic(err)
	}
	encoded := values.Encode()
	return getWithRetry(client, "http://"+location+"/query?"+encoded, 5)
}

func getWithRetry(client *http.Client, url string, attempts int) (*http.Response, error) {
	if attempts == 0 {
		return nil, nil
	}
	resp, err := client.Get(url)
	if err == nil {
		return resp, nil
	} else if attempts > 1 {
		time.Sleep(3 * time.Second)
		return getWithRetry(client, url, attempts-1)
	}
	return nil, err
}

func fetchMeasurements(location, db string) ([]string, error) {
	return fetchShow("MEASUREMENTS", location, db)
}

func fetchDatabases(location string) ([]string, error) {
	return fetchShow("DATABASES", location, "")
}

func fetchRetentionPolicies(location, db string) ([]string, error) {
	return fetchShow("RETENTION POLICIES", location, db)
}

func fetchRetentionPoliciesWithSettings(location, db string) ([]RetentionPolicy, error) {
	results, err := fetchSimple(`SHOW RETENTION POLICIES`, location, db)
	if err != nil {
		return nil, err
	}
	var rps []RetentionPolicy
	for _, row := range results[0].Series {
		for _, value := range row.Values {
			rps = append(rps, RetentionPolicy{
				Name:               value[0].(string),
				Duration:           value[1].(string),
				ShardGroupDuration: value[2].(string),
				Replicas:           int(value[3].(float64)),
				Default:            value[4].(bool),
			})
		}
	}
	return rps, nil
}

func fetchTagKeys(location, db string) (map[string]bool, error) {
	results, err := fetchSimple(`SHOW TAG KEYS`, location, db)
	if err != nil {
		return nil, err
	}
	tags := make(map[string]bool)
	for _, r := range results {
		for _, row := range r.Series {
			for _, values := range row.Values {
				for _, value := range values {
					tags[row.Name+"."+value.(string)] = true
				}
			}
		}
	}
	return tags, nil
}

func fetchShow(name, location, db string) ([]string, error) {
	results, err := fetchSimple(`SHOW `+name, location, db)
	if err != nil {
		return nil, err
	}
	msmts := []string{}
	for _, row := range results[0].Series {
		for _, value := range row.Values {
			msmts = append(msmts, value[0].(string))
		}
	}
	return msmts, nil
}

func fetchSimple(q, location, db string) ([]result, error) {
	resp, err := get(q, location, db, false)
	// TODO handle not able to connect
	if err != nil {
		return nil, err
	}
	respData, readErr := ioutil.ReadAll(resp.Body)
	// TODO handle invalid location
	if readErr != nil {
		return nil, readErr
	}
	var r response
	jsonErr := json.Unmarshal(respData, &r)
	// TODO Figure out what to do if we can't parse the data, which could happen if we are not requesting correctly.
	if err != nil {
		log.Panic(fmt.Errorf("parsing fetched data failed: " + jsonErr.Error()))
	}
	return r.Results, nil
}

func fetchTokenData(token int, location, db, rp string, measurement string) (chan result, error) {
	// TODO limit data retrieved. Also consider allowign checkpointing with smaller amount of data.
	// TODO it should only filter on partition if the measurement has a corresponding partition key.
	return streamData(location, db, rp, measurement, cluster.PartitionTagName+` = '`+strconv.Itoa(token)+`'`)
}

func streamData(location, db, rp string, measurement string, where string) (chan result, error) {
	var stmt = `SELECT * FROM ` + rp + "." + measurement
	if where != "" {
		stmt += ` WHERE ` + where
	}
	ch := make(chan result)

	// Check if it is able to respond to fail fast
	_, err := get(limitQuery(stmt, 1, 0), location, db, true)
	if err != nil {
		return ch, err
	}

	go (func() {
		offset := 0
		limit := 10000
		defer close(ch)

		for {
			var resultCount int
			limitedQuery := limitQuery(stmt, limit, offset)

			// If there is an error here, it is not much we can do.
			resp, err := get(limitedQuery, location, db, true)
			if err != nil {
				return
			}

			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				var r response
				err := json.Unmarshal([]byte(scanner.Text()), &r)
				if err != nil {
					continue
				}
				for _, result := range r.Results {
					ch <- result
					resultCount += len(result.Series)
				}
			}

			offset += limit
			if resultCount == 0 {
				return
			}
		}
	})()
	return ch, nil
}

func limitQuery(query string, limit, offset int) string {
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, limit, offset)
}
