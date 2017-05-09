package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb-relay/relay"
	"log"
	"net/http"
	"io/ioutil"
	"github.com/influxdata/influxdb/models"
	"fmt"
	"strings"
	"bytes"
	"strconv"
	"time"
	"errors"
)

type WriteHandler struct {
	client      *http.Client
	resolver    *cluster.Resolver
	partitioner *cluster.Partitioner
}

func NewWriteHandler(resolver *cluster.Resolver, partitioner *cluster.Partitioner) *WriteHandler {
	client := &http.Client{Timeout: 10 * time.Second}
	return &WriteHandler{client, resolver, partitioner}
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)

	// TODO Need to refactor the creation of relays
	// TODO Find shard key for the specified database and measurement
	// TODO Hash tags as necessary and add as additional tags. If a tag is not included which is needed for the shard key return an error
	// TODO Select the correct replicaset based on sharding or use the default one (first)

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
	precision := query.Get("precision")
	if precision == "" {
		precision = "nanoseconds"
	}

	pointGroups := make(map[int][]models.Point)
	broadcastGroup := []models.Point{}
	for _, point := range points {
		key, ok := h.partitioner.GetKeyByMeasurement(db, point.Name())
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
			numericHash, hashErr := h.partitioner.GetHash(key, values)
			if hashErr != nil {
				jsonError(w, http.StatusInternalServerError, "failed to partition write")
				return
			}
			if _, ok := pointGroups[numericHash]; !ok {
				pointGroups[numericHash] = []models.Point{}
			}
			//point.SetTags(append(point.Tags(), models.Tag{Key: []byte("_partition"), Value: []byte(strconv.FormatInt(int64(numericHash), 10))}))
			pointGroups[numericHash] = append(pointGroups[numericHash], point)
		} else {
			broadcastGroup = append(broadcastGroup, point)
		}
	}

	// get locations to write each each group of points.

	encodedQuery := query.Encode()
	auth := r.Header.Get("Authorization")

	// TODO to all requests in parallel

	if len(broadcastGroup) > 0 {
		locations := h.resolver.FindAll()
		log.Printf("Broacasting write partitioned data to %s", strings.Join(locations, ", "))
		broadcastData := convertPointToBytes(broadcastGroup, precision)
		relayErr := h.relayToLocations(locations, encodedQuery, auth, broadcastData)
		if relayErr != nil {
			panic(relayErr)
		}
	}

	for numericHash, points := range pointGroups {
		data := convertPointToBytes(points, precision)
		locations := h.resolver.FindByKey(numericHash, cluster.READ)
		log.Printf("Writing partitioned data to %s", strings.Join(locations, ", "))
		relayErr := h.relayToLocations(locations, encodedQuery, auth, data)
		if relayErr != nil {
			panic(relayErr)
		}
	}

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

func (h *WriteHandler) relayToLocations(locations []string, query string, auth string, buf []byte) error {
	// TODO handler errors
	// TODO retry on failure
	for _, location := range locations {
		req, err := http.NewRequest("POST", "http://" + location + "/write?" + query, bytes.NewReader(buf))
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
			return responseErr
		}
		if resp.StatusCode != 204 {
			body, rErr := ioutil.ReadAll(resp.Body)
			if rErr != nil {
				log.Fatal(rErr)
			}
			log.Println(string(body))
			return errors.New("Received unexpected response from InfluxDB")
		}
	}
	return nil
}

func createRelayOutput(location string) relay.HTTPOutputConfig {
	output := relay.HTTPOutputConfig{}
	output.Name = location
	output.Location = "http://" + location + "/write"
	return output
}