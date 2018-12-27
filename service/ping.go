package service

import (
	"encoding/json"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/adamringhede/influxdb-ha/syncing"
	"net/http"
	"time"
)

type PingFn func(node cluster.Node) (string, error)

func httpPing(node cluster.Node) (string, error) {
	localClient, _ := syncing.NewInfluxClientHTTPFromNode(node)
	_, version, err := localClient.Ping(10 * time.Second)
	return version, err
}

type PingHandler struct {
	localNode *cluster.Node
	ping      PingFn
}

func (h *PingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	version, err := h.ping(*h.localNode)
	handleInternalError(w, err)

	verbose := r.URL.Query().Get("verbose")

	if verbose != "" && verbose != "0" && verbose != "false" {
		w.WriteHeader(http.StatusOK)
		b, _ := json.Marshal(map[string]string{"version": version})
		_, err := w.Write(b)
		handleInternalError(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func NewPingHandler(localNode *cluster.Node) *PingHandler {
	return &PingHandler{localNode: localNode, ping: httpPing}
}

func (h *PingHandler) OverridePing(ping PingFn) {
	h.ping = ping
}
