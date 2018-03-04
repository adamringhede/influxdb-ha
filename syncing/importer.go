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

type ClusterImportPredicate struct {
	localNode     cluster.Node
	partitionKeys []cluster.PartitionKey
	resolver      *cluster.Resolver
}

// Test returns true if data should be imported for a given database and measurement.
// - test that the measurement has a partitionKey (otherwise it should not be imported by default)
// - test that the database hash resolves to the node given the configured replication factor
// The reason for hashing on database and not measurement is so that queries including multiple measurements
// don't need to be distributed.
func (p *ClusterImportPredicate) Test(db, msmt string) ImportDecision {
	for _, pk := range p.partitionKeys {
		if pk.Database == db && (pk.Measurement == msmt || pk.Measurement == "") {
			return PartitionImport
		}
	}
	key := hash.String(cluster.CreatePartitionKeyIdentifier(db, ""))
	nodes := p.resolver.FindNodesByKey(int(key), cluster.WRITE)
	for _, node := range nodes {
		if node.Name == p.localNode.Name {
			return FullImport
		}
	}
	return NoImport
}

type BasicImporter struct {
	loader    Loader
	Predicate ImportDecisionTester

	// cache
	createdDatabases  map[string]bool
	locationsMeta map[string]locationMeta
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
				i.importTokenData(location, target, token, db, dbMeta)
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

func (i *BasicImporter) forEachDatabase(location string, target string, fn func (db string, dbMeta *databaseMeta)) {
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
			i.createdDatabases[db] = true
		}
		fn(db, dbMeta)
	}
}

func (i *BasicImporter) importTokenData(location, target string, token int, db string, dbMeta *databaseMeta) {
	for _, rp := range dbMeta.rps {
		log.Printf("Exporting data from database %s.%s", db, rp)

		// TODO Do not rely on partition tag for the import
		// Instead, get the partition keys for all measurements that exist on this node.
		// Then request all tag values for these partition keys. Create all possible combinations of these tags and find those that
		// would resolve to the token.
		// The query below shows how we can serch for series for the partition key
		// SHOW TAG VALUES ON "db" WITH KEY IN ("location","randtag")
		for _, msmt := range dbMeta.measurements {
			if importType := i.Predicate(db, msmt); importType == PartitionImport {
				importCh, _ := fetchTokenData(token, location, db, rp, msmt)
				persistStream(importCh, dbMeta, target, db, rp)
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
	// TODO create users and retention policies as well
	resp, err := get("CREATE DATABASE "+db, target, "", false)
	if resp.StatusCode != 200 {
		log.Fatalf("Received invalid status code %d", resp.StatusCode)
	}
	if err != nil {
		log.Panic(err)
	}
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
		tagKeys, err := fetchTagKeys(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.tagKeys = tagKeys
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
	tagKeys      map[string]bool
}

func newDatabaseMeta() *databaseMeta {
	return &databaseMeta{[]string{}, []string{}, map[string]bool{}}
}

func get(q string, location string, db string, chunked bool) (*http.Response, error) {
	client := &http.Client{}
	params := []string{"db=" + db, "q=" + q, "chunked=" + strconv.FormatBool(chunked)}
	values, err := url.ParseQuery(strings.Join(params, "&"))
	if err != nil {
		log.Panic(err)
	}
	encoded := values.Encode()
	return client.Get("http://" + location + "/query?" + encoded)
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
	resp, err := get(stmt, location, db, true)
	if err != nil {
		return nil, err
	}

	ch := make(chan result)
	scanner := bufio.NewScanner(resp.Body)
	go (func() {
		for scanner.Scan() {
			var r response
			err := json.Unmarshal([]byte(scanner.Text()), &r)
			// TODO Figure out what to do if we can't parse the data.
			if err != nil {
				log.Panic(err)
			}
			for _, result := range r.Results {
				ch <- result
			}
		}
		close(ch)
	})()
	return ch, nil
}
