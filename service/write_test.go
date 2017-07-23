package service_test

import (
	"net/url"
	"testing"
	"time"
	"net/http"
	"log"
	"github.com/stretchr/testify/assert"
	"bytes"
	"io/ioutil"
)

func TestRouting(t *testing.T) {
	// TODO start a node locally and run the write query

	client := &http.Client{Timeout: 10 * time.Second}
	//q := `select * from cpu_load_short GROUP BY *`
	values, _ := url.ParseQuery("db=sharded")
	// insert into autogen treasures,type=gold value=29 1439856000
	buf := bytes.NewBuffer([]byte("treasures,type=gold value=29 1439856000"))
	resp, err := client.Post("http://192.168.99.100:8086/write?" + values.Encode(), "text/plain", buf)
	assert.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)
	query, _ := url.ParseQuery("db=sharded&q=SELECT * FROM treasures WHERE type='gold'")
	resp, err = client.Get("http://192.168.99.100:8086/query?" + query.Encode())
	body, rErr := ioutil.ReadAll(resp.Body)
	assert.NoError(t, rErr)
	log.Println(string(body))
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

/*
TODO Test hinted hand off
TODO Test retries
TODO Test different configurations
 */
