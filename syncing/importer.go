package syncing

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	influx "github.com/influxdata/influxdb/client/v2"
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

type ImportBatch struct {
	// index can be used as an offset to continue importing from it.
	index int
}

type Importer interface {
	ImportPartitioned(tokens []int, target InfluxClient)
	ImportNonPartitioned(target InfluxClient)
	DeleteByToken(location InfluxClient, token int) error
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
// The reason for hashing on database and not measurement is so that queries including multiple Measurements
// don't need to be distributed. In the future we may add support for this as it is probably a useful use case.
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

func measurementHasPartitionKey(db, msmt string, pks []cluster.PartitionKey) bool {
	for _, pk := range pks {
		if pk.Database == db && (pk.Measurement == msmt || pk.Measurement == "") {
			return true
		}
	}
	return false
}

type InfluxImporter struct {
	MetaImporter
}

func NewInfluxImporter() *InfluxImporter {
	return &InfluxImporter{}
}

type Checkpoint struct {
	db, rp, msmt string
	timestamp int
}

func NewCheckpoint(db string, rp string, msmt string, timestamp int) *Checkpoint {
	return &Checkpoint{db: db, rp: rp, msmt: msmt, timestamp: timestamp}
}

func (i *InfluxImporter) ImportAll(host, destination InfluxClient, checkpointFn func (checkpoint Checkpoint)) {
	// This does not work when using bookmarks or to filter some of them.
	// This needs to be a completely different component. In some way we need to reuse some functions
	// TODO Mirroring requires authentication as well on both reading and writing so many of these methods need to be rewritten.
	i.forEachDatabase(host, target, func(db string, dbMeta *DatabaseMeta) {
		for _, msmt := range dbMeta.Measurements {
			for _, rp := range dbMeta.Rps {
				importCh, _ := streamData(location, db, rp, msmt, "")
				for res := range importCh {
					persistResult(, dbMeta, target, db, rp)
					checkpointFn(NewCheckpoint(db, rp, msmt, )) // need to get the timestamp from the latest checkpoint
				}

			}
		}
	})
}


type MetaImporter struct {
	createdDatabases map[string]bool
	locationsMeta    map[InfluxClient]locationMeta
}

func (i *MetaImporter) ensureCache() {
	if i.createdDatabases == nil {
		i.createdDatabases = map[string]bool{}
	}
	if i.locationsMeta == nil {
		i.locationsMeta = map[InfluxClient]locationMeta{}
	}
}

func (i *MetaImporter) getLocationsMeta(location InfluxClient) (locationMeta, error) {
	meta, ok := i.locationsMeta[location]
	if !ok {
		fetchedMeta, err := fetchLocationMeta(location)
		if err != nil {
			return fetchedMeta, err
		}
		meta = fetchedMeta
		i.locationsMeta[location] = meta
	}
	return meta, nil
}

func (i *MetaImporter) forEachDatabase(location, target InfluxClient, fn func(db string, dbMeta *DatabaseMeta)) {
	i.ensureCache()
	meta, err := i.getLocationsMeta(location)
	if err != nil {
		log.Printf("Failed fetching meta from location %s. Error: %s", location, err.Error())
		return
	}
	for db, dbMeta := range meta.databases {
		if _, hasDB := i.createdDatabases[db]; !hasDB {
			createDatabase(db, target)
			createRPs(db, dbMeta.RpsSettings, target)
			createCQs(db, dbMeta.Cqs, target)

			i.createdDatabases[db] = true
		}
		fn(db, dbMeta)
	}
}

type ClusterImporter struct {
	MetaImporter
	loader        Loader
	Predicate     ImportDecisionTester
	PartitionKeys cluster.PartitionKeyCollection
	Resolver      *cluster.Resolver
}

func NewImporter(resolver *cluster.Resolver, partitionKeys cluster.PartitionKeyCollection, predicate ImportDecisionTester) *ClusterImporter {
	return &ClusterImporter{Predicate: predicate, Resolver: resolver, PartitionKeys: partitionKeys}
}

func (i *ClusterImporter) ImportNonPartitioned(target InfluxClient) {
	for _, address := range i.Resolver.FindAll() {
		location, _ := NewInfluxClientWithUsingHTTP(address, "", "")
		if location.String() != target.String() {
			i.forEachDatabase(location, target, func(db string, dbMeta *DatabaseMeta) {
				for _, msmt := range dbMeta.Measurements {
					// TODO only import if there is no partition key
					if importType := i.Predicate(db, msmt); importType == FullImport {
						for _, rp := range dbMeta.Rps {
							importCh, _ := streamData(location, db, rp, msmt, "")
							for res := range importCh {
								persistResult(res, dbMeta, target, db, rp)
							}
						}
					}
				}
			})
		}
	}
}

// ImportPartitioned data given a set of tokens. The tokens should include those stolen from
// other nodes as well as token for which this node is holding replicated data
func (i *ClusterImporter) ImportPartitioned(tokens []int, target string) {
	for _, token := range tokens {
		nodes := i.Resolver.FindByKey(token, cluster.READ)
		for _, location := range nodes {
			i.forEachDatabase(location, target, func(db string, dbMeta *DatabaseMeta) {
				i.importTokenData(location, target, token, db, dbMeta, i.Resolver)
			})
		}
	}
	// TODO check that import worked
	// If no location is available at this time, then we have to try again later.
}

func (i *ClusterImporter) importTokenData(location, target string, token int, db string, dbMeta *DatabaseMeta, resolver *cluster.Resolver) {
	for _, rp := range dbMeta.Rps {
		for _, msmt := range dbMeta.Measurements {
			if importType := i.Predicate(db, msmt); importType == PartitionImport {
				for _, series := range dbMeta.series {
					for _, pk := range i.PartitionKeys.GetPartitionKeys() { // fixme this may result in importing daa for the same token multiple times
						if ((pk.Measurement == msmt && pk.Measurement == series.Measurement) || pk.Measurement == "") && series.Matches(token, pk, resolver) {
							importCh, _ := streamData(location, db, rp, msmt, series.Where())
							for res := range importCh {
								persistResult(res, dbMeta, target, db, rp)
							}
						}
					}
				}
			}
		}
	}
}

func (i *ClusterImporter) DeleteByToken(location InfluxClient, token int) error {
	meta, err := i.getLocationsMeta(location)
	if err != nil {
		return fmt.Errorf("failed fetching meta from location %s: %s", location, err.Error())
	}
	for db, dbMeta := range meta.databases {
		for _, rp := range dbMeta.Rps {
			for _, msmt := range dbMeta.Measurements {
				if measurementHasPartitionKey(db, msmt, i.PartitionKeys.GetPartitionKeys()) {
					for _, series := range dbMeta.series {
						for _, pk := range i.PartitionKeys.GetPartitionKeys() {
							if ((pk.Measurement == msmt && pk.Measurement == series.Measurement) || pk.Measurement == "") && series.Matches(token, pk, i.Resolver) {
								err := location.DropSeriesFromWhere(db, msmt, series.Where())
								if err != nil {
									log.Println("Failed to delete data at " + location.String() + " where " + series.Where())
								}
							}
						}
					}
				} else {
					// Test if data for this database should be deleted given the token
					key := hash.String(cluster.CreatePartitionKeyIdentifier(db, ""))
					resolvedToken, _ := i.Resolver.FindTokenByKey(int(key))
					shouldDelete := resolvedToken == token
					if shouldDelete {
						err := location.DropSeriesFrom(db, msmt))
						if err != nil {
							log.Println(fmt.Sprintf("Failed to delete data at %s for measurement %s", location.String(), msmt))
						}
					}
				}
			}
		}
	}
	return nil
}

func persistResult(res influx.Result, dbMeta *DatabaseMeta, target InfluxClient, db, rp string) {
	batch, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Precision: "ns",
		Database: db,
		RetentionPolicy: rp,
		WriteConsistency: "1",
	})
	for _, row := range res.Series {
		// TODO change method to take points instead as it is a better format than generic influx.Result
		batch.AddPoints(convertToPoints(row, dbMeta.TagKeys))
	}
	err := target.Write(batch)
	if err != nil {
		log.Panicf("Failed to post data to target node %s.", target)
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

func createCQs(db string, cqs []ContinuousQuery, target string) {
	for _, cq := range cqs {
		createContinuousQuery(db, cq, target)
	}
}

func createContinuousQuery(db string, cq ContinuousQuery, target string) {
	log.Printf("Creating continuous query %s", cq.Name)
	handleInfluxError(get(cq.Query, target, db, false))
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
			// Avoid saving values as tags
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

		line += " " + strconv.FormatInt(ts.UnixNano(), 10)
		lines = append(lines, line)
	}
	return lines
}


func convertToPoints(row models.Row, tagKeys map[string]bool) []*influx.Point {
	points := []*influx.Point{}
	for _, values := range row.Values {
		tags := map[string]string{}
		i := 1
		// Tags
		for ; i < len(row.Columns)-1; i += 1 {
			// Avoid saving values as tags
			if _, ok := tagKeys[row.Name+"."+row.Columns[i]]; ok {
				if values[i] != nil {
					tags[row.Columns[i]] = values[i].(string)
				}
			} else {
				break
			}
		}
		fields := map[string]interface{};
		// Values
		for ; i < len(row.Columns); i += 1 {
			if values[i] != nil {
				fields[row.Columns[i]] = values[i]
			}
		}
		// Time
		timeStr := values[0].(string)
		ts, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			panic(err)
		}
		point, err := influx.NewPoint(row.Name, tags, fields, ts)
		if err != nil {
			panic(err)
		}
		points = append(points, point)
	}
	return points
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

func fetchLocationMeta(location InfluxClient) (locationMeta, error) {
	meta := newLocationMeta()
	databases, err := fetchDatabases(location)
	if err != nil {
		return meta, err
	}
	for _, db := range databases {
		if db == "_internal" {
			continue
		}
		meta.databases[db] = newDatabaseMeta()
		dbMeta := meta.databases[db]

		measurements, err := fetchMeasurements(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.Measurements = measurements

		rpsSettings, err := location.ShowRetentionPolicies(db)
		if err != nil {
			return meta, err
		}
		dbMeta.RpsSettings = rpsSettings

		rps := []string{}
		for _, rp := range rpsSettings {
			rps = append(rps, rp.Name)
		}
		dbMeta.Rps = rps

		cqs, err := fetchContinuousQueries(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.Cqs = cqs

		tagKeys, err := fetchTagKeys(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.TagKeys = tagKeys

		series, err := FetchSeries(location, db)
		if err != nil {
			return meta, err
		}
		dbMeta.series = series
	}
	return meta, nil
}

type locationMeta struct {
	databases map[string]*DatabaseMeta
}

func newLocationMeta() locationMeta {
	return locationMeta{map[string]*DatabaseMeta{}}
}

type DatabaseMeta struct {
	Measurements []string
	Rps          []string
	RpsSettings  []RetentionPolicy
	Cqs          []ContinuousQuery
	TagKeys      map[string]bool
	series       []Series
}

func newDatabaseMeta() *DatabaseMeta {
	return &DatabaseMeta{
		Measurements: []string{},
		Rps:          []string{},
		RpsSettings:  []RetentionPolicy{},
		Cqs:          []ContinuousQuery{},
		TagKeys:      map[string]bool{},
		series:       []Series{}}
}

type InfluxClient struct {
	influx.Client
	Name string
}

func NewInfluxClient(client influx.Client) *InfluxClient {
	return &InfluxClient{Client: client}
}

func NewInfluxClientWithUsingHTTP(addr, username, password string) (*InfluxClient, error) {
	config := influx.HTTPConfig{
		Addr: addr,
		Username: username,
		Password: password,
	}
	client, err := influx.NewHTTPClient(config)
	if err != nil {
		return nil, err
	}
	return NewInfluxClient(client), nil
}

func (c *InfluxClient) String() string {
	return c.Name
}

func (c *InfluxClient) DropSeriesFrom(db, msmt string) error {
	_, err := c.Query(influx.NewQuery("DROP SERIES FROM " + msmt, db, "ns"))
	return err
}

func (c *InfluxClient) DropSeriesFromWhere(db, msmt string, where string) error {
	_, err := c.Query(influx.NewQuery(fmt.Sprintf("DROP SERIES FROM %s WHERE %s", msmt, where), db, "ns"))
	return err
}

func (c *InfluxClient) ShowMeasurements(db string) ([]string, error) {
	resps, err := c.Query(influx.NewQuery("SHOW MEASUREMENTS", db, "ns"))
	if err != nil {
		return nil, err
	}
	var msmts []string
	for _, row := range resps.Results[0].Series {
		for _, value := range row.Values {
			msmts = append(msmts, value[0].(string))
		}
	}
	return msmts, nil
}

func (c *InfluxClient) ShowRetentionPolicies(db string) ([]RetentionPolicy, error) {
	resps, err := c.Query(influx.NewQuery("SHOW RETENTION POLICIES", db, "ns"))
	if err != nil {
		return nil, err
	}
	var rps []RetentionPolicy
	for _, row := range resps.Results[0].Series {
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

func (c *InfluxClient) ShowContinuousQueries(db string) ([]ContinuousQuery, error) {
	resps, err := c.Query(influx.NewQuery("SHOW CONTINUOUS QUERIES", db, "ns"))
	if err != nil {
		return nil, err
	}
	var cqs []ContinuousQuery
	for _, row := range resps.Results[0].Series {
		for _, value := range row.Values {
			cqs = append(cqs, ContinuousQuery{
				Name:  value[0].(string),
				Query: value[1].(string),
			})
		}
	}
	return cqs, nil
}

func (c *InfluxClient) FetchTagKeys(location, db string) (map[string]bool, error) {
	resps, err := c.Query(influx.NewQuery("SHOW TAG KEYS", db, "ns"))
	if err != nil {
		return nil, err
	}
	tags := make(map[string]bool)
	for _, r := range resps.Results {
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

 // THIS entire component need to be refactored to use the influx client.
 // We could also wrap it to support decoding responses and creating new things.
 // Also, we it should have inbuilt retry support or the ability to configure it.
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

func streamData(location InfluxClient, db, rp string, measurement string, where string) (chan influx.Result, error) {
	var stmt = `SELECT * FROM ` + rp + "." + measurement
	if where != "" {
		stmt += ` WHERE ` + where
	}
	ch := make(chan influx.Result)

	// Check if it is able to respond to fail fast
	_, err := location.Query(influx.NewQuery(limitQuery(stmt, 1, 0), db, "rfc3339"))
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
			resp, err := location.Query(influx.NewQuery(limitedQuery, db, "rfc3339"))
			if err != nil {
				return
			}

			for _, result := range resp.Results {
				ch <- result
				resultCount += len(result.Series)
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
