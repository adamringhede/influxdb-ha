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
	client := &http.Client{Timeout: 10 * time.Second}
	//q := `select * from cpu_load_short GROUP BY *`
	values, _ := url.ParseQuery("db=sharded")
	// insert into autogen treasures,type=gold value=29 1439856000
	buf := bytes.NewBuffer([]byte("treasures,type=gold value=29 1439856000"))
	resp, err := client.Post("http://192.168.99.100:8086/write?" + values.Encode(), "text/plain", buf)
	if err != nil {
		log.Fatal(err)
	}
	body, rErr := ioutil.ReadAll(resp.Body)
	if rErr != nil {
		log.Fatal(rErr)
	}
	log.Println(string(body))
	assert.Equal(t, 200, resp.StatusCode)
}
