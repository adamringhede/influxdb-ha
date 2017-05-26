package sync

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb/models"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Loader interface {
	get(q string, location string, db string, chunked bool)
}


// TODO implement a test loader that just returns json


// TODO create an integration test that actually uses multiple influxdb databses and imports data from them.
type Importer interface {
	// IsActive checks if the node is in progress of importing data
	IsActive() bool
	// Resume importing data
	Resume()
	// Import imports data for a set of nodes
	Import([]int, cluster.Resolver, string)
}

type BasicImporter struct {
	loader Loader
}

type httpLoader struct {
}

func (l *httpLoader) fetch(q, location, db string, chunked bool) {

}

func (l *httpLoader) stream(q, location, db string, chunked bool) {

}

// Import data given a set of tokens. The tokens should include those stolen from
// other nodes as well as token for which this node is holding replicated data
func (i *BasicImporter) Import(tokens []int, resolver *cluster.Resolver, target string) {
	// TODO import and create databases, retention policies, and users.
	// TODO handle consistency issues with different retention policy versions.
	// we may want to save retention policies and users in ETCD as a single source of truth.
	// we can also keep a backlog of items for each node that could not succeed in an update
	//	in that way the node can update its current state incrementally.
	//	another way to to just keep one log forever and have each node keep an index to it.
	//	once in a while an algorithm need to run to make sure its node is up to date.
	createdDatabases := map[string]bool{}
	// For each token resolve nodes
	// Read data from each node and write to local storage.
	locationsMeta := map[string]locationMeta{}
	for _, token := range tokens {
		nodes := resolver.FindByKey(token, cluster.READ)
		// select first node, if it fails, try the next.
		// use an exponential fallback if it continues to fail.
		// at some point though we need to either give up on that token
		// or cancel the import process.

		// NOTE This is assuming that the data is sharded
		for _, location := range nodes {
			log.Printf("Starting to import for token %d from %s", token, location)
			meta, ok := locationsMeta[location]
			if !ok {
				fetchedMeta, err := fetchLocationMeta(location)
				if err != nil {
					log.Printf("Failed fetching meta from location %s. Error: %s", location, err.Error())
					continue
				}
				meta = fetchedMeta
				locationsMeta[location] = meta
			}
			for db, dbMeta := range meta.databases {
				if _, hasDB := createdDatabases[db]; !hasDB {
					log.Printf("Creating database %s", db)
					// TODO create users and retention policies as well
					resp, err := get("CREATE DATABASE " + db, target, "", false)
					if resp.StatusCode != 200 {
						log.Fatalf("Received invalid status code %d", resp.StatusCode)
					}
					if err != nil {
						log.Panic(err)
					}
					createdDatabases[db] = true
				}
				for _, rp := range dbMeta.rps {
					log.Printf("Exporting data from database and RP %s.%s", db, rp)
					importCh, _ := fetchTokenData(token, location, db, rp, strings.Join(dbMeta.measurements, ","))
					for res := range importCh {
						lines := []string{}
						for _, row := range res.Series {

							lines = append(lines, parseLines(row, dbMeta.tagKeys)...)
						}
						_, err := postLines(target, db, rp, lines)
						if err != nil {
							// TODO handle any type of failure to write locally.
							log.Panic("Failed to post data to local node.")
						}
					}
				}
			}
			// TODO find a better way to structure this to handle errors
			break
		}
	}
	log.Println("Finished import")
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
	case float64: return strconv.FormatFloat(value.(float64), 'f', 6, 64)
	case float32: return strconv.FormatFloat(float64(value.(float32)), 'f', 6, 32)
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
		log.Panic(jsonErr)
	}
	return r.Results, nil
}

func fetchTokenData(token int, location, db, rp string, measurement string) (chan result, error) {
	resp, err := get(
		`SELECT * FROM `+rp+"."+measurement+` WHERE _partitionToken = '`+strconv.Itoa(token)+`'`,
		location,
		db, true)
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
