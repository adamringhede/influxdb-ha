package router

import (
	"net/http"
	"log"
	"io/ioutil"
	"io"
	"bytes"
	"net/url"
	"math/rand"
	"time"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb-relay/relay"
)

type host struct {
	uri string
}

type targets struct {
	relay []host
	replica []host
}

func passBack(w *http.ResponseWriter, res *http.Response) {
	defer res.Body.Close()
	for k, v := range res.Header {
		for _, h := range v {
			(*w).Header().Set(k, h)
		}
	}
	(*w).WriteHeader(res.StatusCode)
	flusher, _ := (*w).(http.Flusher)
	_, err := io.Copy(*w, res.Body)
	flusher.Flush()
	if err != nil {
		log.Fatal(err)
	}
}

func handleRouteError(target string, err error) {
	if err != nil {
		log.Fatal("route " + target + ": ", err)
	}
}

type Resolver interface {
	GetDataNodes()
}
type HTTPHandler struct {
	client *http.Client
	config *RouterConfig
}
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal("request",err)
	}
	log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)
	if r.URL.Path == "/write" {

		/*

		Once we know the replica set to use by sharding (if needed),
		then we can use a local relay for it and pass on the request.

		relayHttpConfig := relay.HTTPConfig{}
		relayHttp, relayErr := relay.NewHTTP(relayHttpConfig)
		if relayErr != nil {
			log.Panic(relayErr)
		}
		relayHttp.(relay.HTTP).ServeHTTP(w, r)
		*/

		// TODO Find shard key for the specified database and measurement
		// TODO Hash tags as necessary and add as additional tags. If a tag is not included which is needed for the shard key return an error

		// send to any relay until a 200 response is received
		// if shard is enabled, select the right replica set
		// TODO the config should be one router only
		// TODO validate that the configuration includes at least one relay
		relays := h.config.Relays
		for i, relayConf := range relays {
			res, err := h.client.Post("http://" + relayConf.Location + "/write?" + r.URL.RawQuery, "application/octet-stream", bytes.NewBuffer(buf))
			if res.StatusCode == 200 || i == len(relays) - 1 {
				handleRouteError("write", err)
				passBack(&w, res)
				break
			}
		}

	} else {
		// send to any replica
		hosts := h.config.Data

		// when the selected measurement is sharded, the correct replicaset needs to be found
		// (non sharded measurements are only on one replicaset and need to lookup in a config server what shard they are on)
		// a set of hosts need to be resolved for the shard's replicate
		// the shared is selected based on the given shard key.
		// if the query does not match the any shard key,

		// TODO Find the chunk based on a shard key matching the query Option.
		// TODO Get the replicaset hosts of the shard holding that shard
		// TODO Select one of those hosts and pass on the query.



		queryParam := r.URL.Query().Get("q")
		if queryParam != "" {
			q, parseErr := influxql.ParseQuery(r.URL.Query()["q"][0])
			if parseErr != nil {
				log.Panic(err)
			}
			selectStatements := []*influxql.SelectStatement{}
			selectStatement := ""
			otherStatements := influxql.Statements{}
			for _, stmt := range q.Statements {
				switch s := stmt.(type) {
				case *influxql.SelectStatement:
					// Select statements may need to be sent to different nodes
					selectStatements = append(selectStatements, s)
					selectStatement = selectStatement + s.String() + "; "
				default:
					// All other statements should be sent to all nodes.
					otherStatements = append(otherStatements, s)
				}
			}
		}


		/*
		if the query includes a statement for a sharded measurement,
		then that query needs to be processed separately and appended to the result.
		 */


		/*
		this need to be fixed. need to decide what statements should be
		sent to all hosts. read statements should only be sent to the first
		host. things that are specific to each host should not be allowed
		to access through the router, such as dropping a shard. or we implement
		support for those statements as well. some like SHOW SERIES should
		be sent to all shards and be concatenated, but only to one host in each
		replicaset.
		 */

		/*if len(otherStatements) > 0 {
			for i, host := range hosts {
				baseUrl, _ := url.Parse("http://" + host + r.URL.Path)
				queryValues := r.URL.Query()
				queryValues.Set("q", otherStatements.String())
				baseUrl.RawQuery = queryValues.Encode()
				res, err := client.Get(baseUrl.String())
				handleRouteError("query", err)
				if i == len(hosts) - 1 {
					passBack(&w, res)
				}
			}
		}*/
		selectedHost := hosts[rand.Intn(len(hosts))]
		log.Print("Selected host " + selectedHost.Name + " at " + selectedHost.Location)
		baseUrl, _ := url.Parse("http://" + selectedHost.Location + r.URL.Path)
		//queryValues := r.URL.Query()
		//queryValues.Set("q", selectStatement)
		baseUrl.RawQuery = r.URL.Query().Encode()
		res, err := h.client.Get(baseUrl.String())
		handleRouteError("query", err)
		passBack(&w, res)
	}
}


func Start(config *RouterConfig) {
	addr := ":5096"
	if config.BindAddr != "" {
		addr = config.BindAddr
	}
	client := &http.Client{Timeout: 10 * time.Second}
	http.Handle("/", &HTTPHandler{client, config})

	log.Println("Listening on " + addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
