package router_test

import (
	"encoding/json"
	"github.com/adamringhede/influxdb-ha/router"
	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// Message represents a user-facing message to be included with the result.
type Message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

type Result struct {
	StatementID int           `json:"statement_id"`
	Series      []*models.Row `json:"series,omitempty"`
	Messages    []*Message    `json:"messages,omitempty"`
	Partial     bool          `json:"partial,omitempty"`
	Err         string        `json:"error,omitempty"`
}

type Response struct {
	Results []Result `json:"results"`
}

var client = &http.Client{Timeout: 120 * time.Second}

func parseResp(resp *http.Response) Response {
	var r Response
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&r)
	return r
}

func TestRouting(t *testing.T) {
	config, _ := router.ParseConfigFile("../rs.toml")
	go router.Start(&config.Routers[0], config.ReplicaSets)
	//q := "SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='us-west' AND \"host\"='server01'"
	q := `select * from cpu_load_short GROUP BY *`
	values, _ := url.ParseQuery("chunked=true&db=r&q=" + q)
	query := values.Encode()
	resp, err := client.Get("http://localhost:5096/query?" + query)
	if err != nil {
		log.Fatal(err)
	}
	x := parseResp(resp)
	assert.Equal(t, 1, len(x.Results))
	assert.Equal(t, 200, resp.StatusCode)
}
