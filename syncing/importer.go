package syncing

import (
	"bytes"
	"encoding/json"
	"fmt"
	influx "github.com/influxdata/influxdb/client/v2"
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
	ImportPartitioned(tokens []int, target *InfluxClient)
	ImportNonPartitioned(target *InfluxClient)
	DeleteByToken(location *InfluxClient, token int) error
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

type ImportBookmark interface {
	Set(db, rp, measurement string, timestamp int, done bool)
	Get(db, rp, measurement string) (int, bool, bool)
}

type InfluxImporter struct {
	MetaImporter
}

func NewInfluxImporter() *InfluxImporter {
	return &InfluxImporter{}
}

func (i *InfluxImporter) ImportAll(location, target *InfluxClient, bookmark ImportBookmark) {
	errorCount := 0
	i.forEachDatabase(location, target, func(db string, dbMeta *DatabaseMeta) {
		for _, msmt := range dbMeta.Measurements {
			for _, rp := range dbMeta.Rps {
				offsetTime, _, _ := bookmark.Get(db, rp, msmt)
				importCh, err := streamData(location, db, rp, msmt, fmt.Sprintf("time > '%s'", time.Unix(0, int64(offsetTime)).Format(time.RFC3339)))
				if err != nil {
					fmt.Printf("Failed to fetch data: %s", err.Error())
					errorCount++
					if errorCount > 10 {
						fmt.Printf("Received 10 errors during import from %s and giving up.", db)
						return
					}
				}
				for res := range importCh {
					points := convertResultToPoints(res, dbMeta)
					if len(points) > 0 {
						writePoints(points, target, db, rp)
						bookmark.Set(db, rp, msmt, int(points[len(points)-1].Time().UnixNano()), false)
					}
				}
			}
		}
	})
}

type MetaImporter struct {
	createdDatabases map[string]bool
	locationsMeta    map[*InfluxClient]locationMeta
}

func (i *MetaImporter) ensureCache() {
	if i.createdDatabases == nil {
		i.createdDatabases = map[string]bool{}
	}
	if i.locationsMeta == nil {
		i.locationsMeta = map[*InfluxClient]locationMeta{}
	}
}

func (i *MetaImporter) getLocationsMeta(location *InfluxClient) (locationMeta, error) {
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

func (i *MetaImporter) forEachDatabase(location, target *InfluxClient, fn func(db string, dbMeta *DatabaseMeta)) {
	i.ensureCache()
	meta, err := i.getLocationsMeta(location)
	if err != nil {
		log.Printf("Failed fetching meta from location %s. Error: %s", location, err.Error())
		return
	}
	for db, dbMeta := range meta.databases {
		if _, hasDB := i.createdDatabases[db]; !hasDB {
			target.CreateDatabase(db)
			target.CreateRetentionPolicies(db, dbMeta.RpsSettings)
			target.CreateContinuousQueries(db, dbMeta.Cqs)
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

func (i *ClusterImporter) ImportNonPartitioned(target *InfluxClient) {
	for _, address := range i.Resolver.FindAll() {
		location, _ := NewInfluxClientHTTP(address, "", "")
		if location.String() != target.String() {
			i.forEachDatabase(location, target, func(db string, dbMeta *DatabaseMeta) {
				for _, msmt := range dbMeta.Measurements {
					// TODO only import if there is no partition key
					if importType := i.Predicate(db, msmt); importType == FullImport {
						for _, rp := range dbMeta.Rps {
							importCh, _ := streamData(location, db, rp, msmt, "")
							for res := range importCh {
								points := convertResultToPoints(res, dbMeta)
								writePoints(points, target, db, rp)
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
func (i *ClusterImporter) ImportPartitioned(tokens []int, target *InfluxClient) {
	for _, token := range tokens {
		nodes := i.Resolver.FindNodesByKey(token, cluster.READ)
		for _, node := range nodes {
			locationClient, _ := NewInfluxClientHTTPFromNode(*node)
			i.forEachDatabase(locationClient, target, func(db string, dbMeta *DatabaseMeta) {
				i.importTokenData(locationClient, target, token, db, dbMeta, i.Resolver)
			})
		}
	}
	// TODO check that import worked
	// If no location is available at this time, then we have to try again later.
}

func (i *ClusterImporter) importTokenData(location, target *InfluxClient, token int, db string, dbMeta *DatabaseMeta, resolver *cluster.Resolver) {
	for _, rp := range dbMeta.Rps {
		for _, msmt := range dbMeta.Measurements {
			if importType := i.Predicate(db, msmt); importType == PartitionImport {
				for _, series := range dbMeta.series {
					for _, pk := range i.PartitionKeys.GetPartitionKeys() { // fixme this may result in importing daa for the same token multiple times
						if ((pk.Measurement == msmt && pk.Measurement == series.Measurement) || pk.Measurement == "") && series.Matches(token, pk, resolver) {
							importCh, _ := streamData(location, db, rp, msmt, series.Where())
							for res := range importCh {
								points := convertResultToPoints(res, dbMeta)
								writePoints(points, target, db, rp)
							}
						}
					}
				}
			}
		}
	}
}

func (i *ClusterImporter) DeleteByToken(location *InfluxClient, token int) error {
	meta, err := i.getLocationsMeta(location)
	if err != nil {
		return fmt.Errorf("failed fetching meta from location %s: %s", location, err.Error())
	}
	for db, dbMeta := range meta.databases {
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
					err := location.DropSeriesFrom(db, msmt)
					if err != nil {
						log.Println(fmt.Sprintf("Failed to delete data at %s for measurement %s", location.String(), msmt))
					}
				}
			}
		}
	}
	return nil
}

func writePoints(points []*influx.Point, target *InfluxClient, db, rp string) {
	batch, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Precision:        "ns",
		Database:         db,
		RetentionPolicy:  rp,
		WriteConsistency: "",
	})
	batch.AddPoints(points)
	err := target.Write(batch)
	if err != nil {
		log.Panicf("Failed to post data to target node %s: %s", target, err)
	}
}

func (c *InfluxClient) CreateDatabase(db string) error {
	_, err := c.Query(influx.NewQuery("CREATE DATABASE "+db, "", "ns"))
	return err
}

func (c *InfluxClient) CreateRetentionPolicy(db string, rp RetentionPolicy) error {
	q := fmt.Sprintf(`CREATE RETENTION POLICY "%s" ON "%s" DURATION %s REPLICATION 1`, rp.Name, db, rp.Duration)
	_, err := c.Query(influx.NewQuery(q, "", "ns"))
	return err
}

func (c *InfluxClient) CreateRetentionPolicies(db string, rps []RetentionPolicy) (err error) {
	for _, rp := range rps {
		err = c.CreateRetentionPolicy(db, rp)
	}
	return
}

func (c *InfluxClient) CreateContinuousQuery(db string, cq ContinuousQuery) error {
	_, err := c.Query(influx.NewQuery(cq.Query, "", "ns"))
	return err
}

func (c *InfluxClient) CreateContinuousQueries(db string, cqs []ContinuousQuery) (err error) {
	for _, cq := range cqs {
		err = c.CreateContinuousQuery(db, cq)
	}
	return
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
	client := http.Client{Timeout: 60 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func convertResultToPoints(result influx.Result, dbMeta *DatabaseMeta) []*influx.Point {
	points := []*influx.Point{}
	for _, row := range result.Series {
		points = append(points, convertRowToPoints(row, dbMeta.TagKeys)...)
	}
	return points
}

func convertRowToPoints(row models.Row, tagKeys map[string]bool) []*influx.Point {
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
		fields := map[string]interface{}{}
		// Values
		for ; i < len(row.Columns); i += 1 {
			if values[i] != nil {
				fields[row.Columns[i]] = values[i]
			}
		}
		// Time
		ts, err := values[0].(json.Number).Int64()
		if err != nil {
			panic(err)
		}
		point, err := influx.NewPoint(row.Name, tags, fields, time.Unix(0, ts))
		if err != nil {
			panic(err)
		}
		points = append(points, point)
	}
	return points
}

func fetchLocationMeta(location *InfluxClient) (locationMeta, error) {
	meta := newLocationMeta()
	databases, err := location.ShowDatabases()
	if err != nil {
		return meta, err
	}
	for _, db := range databases {
		if db == "_internal" {
			continue
		}
		meta.databases[db] = newDatabaseMeta()
		dbMeta := meta.databases[db]

		measurements, err := location.ShowMeasurements(db)
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

		cqs, err := location.ShowContinuousQueries(db)
		if err != nil {
			return meta, err
		}
		dbMeta.Cqs = cqs

		tagKeys, err := location.FetchTagKeys(db)
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
	Name     string
	Location string
}

func NewInfluxClient(client influx.Client, name string, location string) *InfluxClient {
	return &InfluxClient{Client: client, Name: name, Location: location}
}

func NewInfluxClientHTTP(addr, username, password string) (*InfluxClient, error) {
	config := influx.HTTPConfig{
		Addr:     "http://" + addr,
		Username: username,
		Password: password,
	}
	client, err := influx.NewHTTPClient(config)
	if err != nil {
		panic(err)
		return nil, err
	}
	return NewInfluxClient(client, fmt.Sprintf("%s@%s", username, addr), addr), nil
}

func NewInfluxClientHTTPFromNode(node cluster.Node) (*InfluxClient, error) {
	return NewInfluxClientHTTP(node.DataLocation, "", "")
}

func (c *InfluxClient) String() string {
	return fmt.Sprintf("Influx(Name=%s, Location=%s)", c.Name, c.Location)
}

func (c *InfluxClient) DropSeriesFrom(db, msmt string) error {
	_, err := c.Query(influx.NewQuery("DROP SERIES FROM "+msmt, db, "ns"))
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

func (c *InfluxClient) ShowDatabases() ([]string, error) {
	resps, err := c.Query(influx.NewQuery("SHOW DATABASES", "", "ns"))
	if err != nil {
		return nil, err
	}
	var dbs []string
	for _, row := range resps.Results[0].Series {
		for _, value := range row.Values {
			dbs = append(dbs, value[0].(string))
		}
	}
	return dbs, nil
}

func (c *InfluxClient) ShowRetentionPolicies(db string) ([]RetentionPolicy, error) {
	resps, err := c.Query(influx.NewQuery("SHOW RETENTION POLICIES", db, "ns"))
	if err != nil {
		return nil, err
	}
	var rps []RetentionPolicy
	for _, row := range resps.Results[0].Series {
		for _, value := range row.Values {
			replicas, err := value[3].(json.Number).Int64()
			if err != nil {
				log.Panicf("failed to parse replica count from: %v", value[3])
			}
			rps = append(rps, RetentionPolicy{
				Name:               value[0].(string),
				Duration:           value[1].(string),
				ShardGroupDuration: value[2].(string),
				Replicas:           int(replicas),
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

func (c *InfluxClient) FetchTagKeys(db string) (map[string]bool, error) {
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

func streamData(location *InfluxClient, db, rp string, measurement string, where string) (chan influx.Result, error) {
	var stmt = `SELECT * FROM ` + rp + "." + measurement
	if where != "" {
		stmt += ` WHERE ` + where
	}
	stmt += " ORDER BY time asc"
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
