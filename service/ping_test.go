package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

const testPingVersion = "1.7.2"

var failingPing = func(node cluster.Node) (string, error) {
	return "", errors.New("test error")
}

var workingPing = func(node cluster.Node) (string, error) {
	return testPingVersion, nil
}

func TestPingHandler_ServeHTTP_failing(t *testing.T) {
	handler := setupPingTest(failingPing)
	resp := testPing(handler, "1")
	assert.Equal(t, 500, resp.StatusCode)
}

func TestPingHandler_ServeHTTP_working(t *testing.T) {
	handler := setupPingTest(workingPing)
	resp := testPing(handler, "1")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, testPingVersion, parseResponse(resp).Version)
}

func setupPingTest(fn PingFn) *PingHandler {
	node := &cluster.Node{}
	handler := NewPingHandler(node)
	handler.OverridePing(fn)
	return handler
}

func testPing(handler http.Handler, verbose string) *http.Response {
	req := httptest.NewRequest("GET", fmt.Sprintf("http://localhost/ping?verbose=%s", verbose), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w.Result()
}

func parseResponse(r *http.Response) pingResponse {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	var ping pingResponse
	err = json.Unmarshal(data, &ping)
	if err != nil {
		panic(err)
	}
	return ping
}

type pingResponse struct {
	Version string `json:"version"`
}