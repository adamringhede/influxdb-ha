package router_test

import (
	"testing"
	"github.com/adamringhede/influxdb-ha/router"
	"net/http"
	"log"
	"github.com/stretchr/testify/assert"
	"net/url"
	"encoding/json"
	"github.com/influxdata/influxdb/models"
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
	Results	[]Result	`json:"results"`
}

var client = &http.Client{Timeout: 120 * time.Second}

func parseResp(resp *http.Response) Response {
	var r Response
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&r)
	return r
}

func TestRouting(t *testing.T) {
	config, _ := router.ParseConfigFile("../ha-router.toml")
	go router.Start(&config.Routers[0])
	values, _ := url.ParseQuery("db=mydb&q=SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='us-west' AND \"host\"='server01'")
	query := values.Encode()
	resp, err := client.Get("http://localhost:5096/query?" + query)
	if err != nil {
		log.Panic(err)
	}
	x := parseResp(resp)
	assert.Equal(t, 1, len(x.Results))
	assert.Equal(t, 200, resp.StatusCode)
}
