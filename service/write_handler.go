package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb-relay/relay"
	"log"
	"net/http"
)

type WriteHandler struct {
	client   *http.Client
	resolver *cluster.Resolver
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)

	// TODO Need to refactor the creation of relays
	// TODO Find shard key for the specified database and measurement
	// TODO Hash tags as necessary and add as additional tags. If a tag is not included which is needed for the shard key return an error
	// TODO Select the correct replicaset based on sharding or use the default one (first)

	outputs := []relay.HTTPOutputConfig{}
	for _, location := range h.resolver.FindAll() {
		output := relay.HTTPOutputConfig{}
		output.Name = location
		output.Location = "http://" + location + "/write"
		outputs = append(outputs, output)
	}

	relayHttpConfig := relay.HTTPConfig{}
	relayHttpConfig.Name = ""
	relayHttpConfig.Outputs = outputs
	relayHttp, relayErr := relay.NewHTTP(relayHttpConfig)
	if relayErr != nil {
		log.Panic(relayErr)
	}
	relayHttp.(*relay.HTTP).ServeHTTP(w, r)
}
