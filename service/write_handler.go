package service

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/influxdata/influxdb/services/meta"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

type WriteHandler struct {
	resolver        *cluster.Resolver
	partitioner     cluster.Partitioner
	authService     AuthService
	pointsWriter    PointsWriter
}

func NewWriteHandler(resolver *cluster.Resolver, partitioner cluster.Partitioner, authService AuthService, pointsWriter PointsWriter) *WriteHandler {
	return &WriteHandler{
		resolver,
		partitioner,
		authService,
		pointsWriter,
	}
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)

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
	body := r.Body

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "unable to parse points")
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if r.ContentLength > 0 {
		// This will just be an initial hint for the gzip reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)
	_, err := buf.ReadFrom(body)

	points, err := models.ParsePoints(buf.Bytes())
	if err != nil {
		jsonError(w, http.StatusBadRequest, "unable to parse points")
		return
	}

	if h.authService != nil {
		user, err := authenticate(r, h.authService)
		if user == nil {
			// The user must be authenticated to write.
			err = meta.ErrAuthenticate
		}
		if err != nil {
			handleErrorWithCode(w, err, http.StatusUnauthorized)
			return
		}

		if !user.AuthorizeDatabase(influxql.WritePrivilege, db) {
			jsonError(w, http.StatusForbidden, "forbidden to write to database: "+db) // TODO Get the corret error message
			return
		}
		for _, point := range points {
			if !user.AuthorizeSeriesWrite(db, point.Name(), point.Tags()) {
				jsonError(w, http.StatusForbidden, "forbidden to write to measurement "+db+"."+string(point.Name()))
				return
			}
		}
	}

	pointGroups, err := partitionPoints(points, h.partitioner, db)
	if err != nil {
		switch err.(type) {
		case partitionValidationError:
			jsonError(w, http.StatusBadRequest, err.Error())
		default:
			jsonError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	// auth := r.Header.Get("Authorization")
	// TODO Handle the case that the underlying InfluxDB instances requires authentication.

	writeContext := WriteContext{
		precision: precision,
		db:        db,
		rp:        rp,
	}

	wg := sync.WaitGroup{}
	wg.Add(len(pointGroups))
	var writeErr error
	for numericHash, points := range pointGroups {
		go (func(numericHash int, points []models.Point) {
			locations := h.resolver.FindNodesByKey(numericHash, cluster.WRITE)
			relayErr := h.pointsWriter.WritePoints(points, locations, writeContext)
			if relayErr != nil {
				writeErr = relayErr
				log.Printf("Failed to write: %s\n", relayErr.Error())
			}
			wg.Done()
		})(numericHash, points)
	}
	wg.Wait()

	if writeErr != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Sprintf("One ore more writes failed: %s", writeErr.Error()))
		return
	}
	fmt.Printf("Wrote %d points", len(points))

	w.WriteHeader(http.StatusNoContent)
}

type HttpPointsWriter struct {
	client          *http.Client
	recoveryStorage cluster.RecoveryStorage
}

func NewHttpPointsWriter(recoveryStorage cluster.RecoveryStorage) *HttpPointsWriter {
	return &HttpPointsWriter{&http.Client{Timeout: 10 * time.Second}, recoveryStorage}
}

func (w *HttpPointsWriter) WritePoints(points []models.Point, locations []*cluster.Node, wc WriteContext) error {
	data := convertPointToBytes(points, wc.precision)
	relayErr := w.relayToLocations(locations, "", data, wc.db, wc.rp)
	if relayErr != nil {
		log.Printf("Failed to write: %s\n", relayErr.Error())
	}
	return relayErr
}

func convertPointToBytes(points []models.Point, precision string) []byte {
	pointsString := ""
	for _, point := range points {
		pointsString += point.PrecisionString(precision) + "\n"
	}
	return []byte(pointsString)
}

func (w *HttpPointsWriter) relayToLocations(nodes []*cluster.Node, auth string, buf []byte, db, rp string) error {
	var err error
	for _, node := range nodes {
		location := node.DataLocation

		// TODO Create a proper http client for requesting InfluxDB to also support SSL and authentication
		var url string
		if rp != "" {
			url = fmt.Sprintf("http://%s/write?db=%s&rp=%s", location, db, rp)
		} else {
			url = fmt.Sprintf("http://%s/write?db=%s", location, db)
		}
		req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, responseErr := w.client.Do(req)
		if responseErr != nil {
			rErr := w.recoveryStorage.Put(node.Name, db, rp, buf)
			if rErr != nil {
				log.Printf("Recovery storage failed: %s\n", rErr.Error())
			}
			err = responseErr
		} else if resp.StatusCode != 204 {
			body, rErr := ioutil.ReadAll(resp.Body)
			if rErr != nil {
				log.Fatal(rErr)
			}
			err = fmt.Errorf("received error from InfluxDB at %s: %s", location, string(body))
		}
	}
	return err
}

const maxWriteRetries = 10
const retryTimeoutSeconds = 5

func (h *WriteHandler) retryWrite(req *http.Request) bool {
	client := http.Client{Timeout: time.Second * 5}
	retries := 0
	for {
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
