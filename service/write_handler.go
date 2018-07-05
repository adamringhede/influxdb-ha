package service

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb-relay/relay"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

type WriteHandler struct {
	client          *http.Client
	resolver        *cluster.Resolver
	partitioner     cluster.Partitioner
	recoveryStorage cluster.RecoveryStorage
	authService     AuthService
}

func NewWriteHandler(resolver *cluster.Resolver, partitioner cluster.Partitioner, rs cluster.RecoveryStorage, authService AuthService) *WriteHandler {
	client := &http.Client{Timeout: 10 * time.Second}
	return &WriteHandler{client, resolver, partitioner, rs, authService}
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)

	user, err := authenticate(r, h.authService)
	if err != nil {
		handleErrorWithCode(w, err, http.StatusUnauthorized)
		return
	}

	buf, err := ioutil.ReadAll(r.Body)
	points, err := models.ParsePoints(buf)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "unable to parse points")
		return
	}
	// group points by hash. The reason for hash and location is because we need to handle hinted handoff
	// which is derived from the hash.
	query := r.URL.Query()
	db := query.Get("db")
	if db == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: db")
		return
	}
	// Unless rp is defined, it will be "" which should result in the default RP for the DB.
	rp := query.Get("rp")

	precision := query.Get("precision")
	if precision == "" {
		precision = "nanoseconds"
	}


	if !user.AuthorizeDatabase(influxql.WritePrivilege, db) {
		jsonError(w, http.StatusForbidden, "forbidden to write to database: " + db) // TODO Get the corret error message
		return
	}
	for _, point := range points {
		if !user.AuthorizeSeriesWrite(db, point.Name(), point.Tags()) {
			jsonError(w, http.StatusForbidden, "forbidden to write to measurement" + db + "." + string(point.Name()))
			return
		}
	}

	pointGroups := make(map[int][]models.Point)
	broadcastGroup := []models.Point{}
	for _, point := range points {
		key, ok := h.partitioner.GetKeyByMeasurement(db, string(point.Name()))
		if ok {
			values := make(map[string][]string)
			for _, tag := range point.Tags() {
				values[string(tag.Key)] = []string{string(tag.Value)}
			}
			if !h.partitioner.FulfillsKey(key, values) {
				jsonError(w, http.StatusBadRequest,
					fmt.Sprintf("the partition key for measurement %s requires the tags [%s]",
						key.Measurement, strings.Join(key.Tags, ", ")))
				return
			}
			// TODO add support for multiple
			numericHash, hashErr := cluster.GetHash(key, values)
			if hashErr != nil {
				jsonError(w, http.StatusInternalServerError, "failed to partition write")
				return
			}
			if _, ok := pointGroups[numericHash]; !ok {
				pointGroups[numericHash] = []models.Point{}
			}
			pointGroups[numericHash] = append(pointGroups[numericHash], point)
		} else {
			broadcastGroup = append(broadcastGroup, point)
		}
	}

	encodedQuery := query.Encode()
	auth := r.Header.Get("Authorization")

	wg := sync.WaitGroup{}

	var writeErr error
	if len(broadcastGroup) > 0 {
		wg.Add(1)
		locations := h.resolver.FindAllNodes()
		broadcastData := convertPointToBytes(broadcastGroup, precision)
		go (func() {
			relayErr := h.relayToLocations(locations, encodedQuery, auth, broadcastData, db, rp)
			if relayErr != nil {
				writeErr = writeErr
				log.Printf("Failed to write: %s\n", relayErr.Error())
			}
			wg.Done()
		})()
	}

	wg.Add(len(pointGroups))
	for numericHash, points := range pointGroups {
		go (func() { // TODO make the relaying into a channel so that we don't consume too much memory here
			data := convertPointToBytes(points, precision)
			locations := h.resolver.FindNodesByKey(numericHash, cluster.WRITE)
			relayErr := h.relayToLocations(locations, encodedQuery, auth, data, db, rp)
			if relayErr != nil {
				writeErr = relayErr
				log.Printf("Failed to write: %s\n", relayErr.Error())
			}
			wg.Done()
		})()
	}
	if writeErr != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("One ore more writes failed: %s", writeErr.Error()))
		return
	}

	wg.Wait()

	w.WriteHeader(http.StatusNoContent)

	/*outputs := []relay.HTTPOutputConfig{}
	for _, location := range h.resolver.FindAll() {
		outputs = append(outputs, createRelayOutput(location))
	}

	relayHttpConfig := relay.HTTPConfig{}
	relayHttpConfig.Name = ""
	relayHttpConfig.Outputs = outputs
	relayHttp, relayErr := relay.NewHTTP(relayHttpConfig)
	if relayErr != nil {
		log.Panic(relayErr)
	}
	relayHttp.(*relay.HTTP).ServeHTTP(w, r)
	*/
}

func convertPointToBytes(points []models.Point, precision string) []byte {
	pointsString := ""
	for _, point := range points {
		pointsString += point.PrecisionString(precision) + "\n"
	}
	return []byte(pointsString)
}

func (h *WriteHandler) relayToLocations(nodes []*cluster.Node, query string, auth string, buf []byte, db, rp string) error {
	var err error
	for _, node := range nodes {
		location := node.DataLocation
		req, err := http.NewRequest("POST", "http://"+location+"/write?"+query, bytes.NewReader(buf))
		if err != nil {
			return err
		}

		req.URL.RawQuery = query
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, responseErr := h.client.Do(req)
		if responseErr != nil {
			// Retry in case of temporary issue. For longer downtime, the recipient needs
			// another way to recover the lost write.
			// The data (buf) needs to be written to some reliable storage (appended to a file)
			// and the recipient needs to know where this data is and import it.
			// However, if the location the data is written to is not available when the node
			// recovers, the data may be lost forever.

			// One solution to start with could be to just store it locally if the retry failed
			// Once the target node recovers, we need to figure out if it should still have the data
			// to send *

			// High availability setup with load balancing but without partitioning
			// Set the replication factor to a value equal to or greater than the number of nodes
			// Once a new node joins the cluster. It will steal tokens in order to become the primary
			// of them. This is necessary as it

			// An alternative to the high availability setup is to not rely on tokens at all.
			// It would then write everything to every node.
			//go (func() {
			// Retry the write before putting it in recovery storage
			//success := h.retryWrite(req)
			//if !success {
			// TODO figure out if it is necessary to use database and rp
			// or if this data is already saved in each point or if we can add defaults to the points.
			rErr := h.recoveryStorage.Put(node.Name, db, rp, buf)
			if rErr != nil {
				log.Printf("Recovery storage failed: %s\n", rErr.Error())
			}
			//}
			//})()
			err = responseErr
			continue
		}
		if resp.StatusCode != 204 {
			body, rErr := ioutil.ReadAll(resp.Body)
			if rErr != nil {
				log.Fatal(rErr)
			}
			err = fmt.Errorf("Received error from InfluxDB at %s: %s", location, string(body))
		}

	}
	return err
}

func createRelayOutput(location string) relay.HTTPOutputConfig {
	output := relay.HTTPOutputConfig{}
	output.Name = location
	output.Location = "http://" + location + "/write"
	return output
}

const maxWriteRetries = 10
const retryTimeoutSeconds = 5

func (h *WriteHandler) retryWrite(req *http.Request) bool {
	client := http.Client{Timeout: time.Second * 5}
	retries := 0
	for true {
		retries += 1
		resp, _ := client.Do(req)
		if resp != nil && resp.StatusCode != 204 {
			return true
		}
		if retries >= maxWriteRetries {
			break
		}
		time.Sleep(time.Second * retryTimeoutSeconds)
	}
	return false
}
