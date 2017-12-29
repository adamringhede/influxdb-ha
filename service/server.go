package service

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/adamringhede/influxdb-ha/cluster"
)

type Config struct {
	BindAddr string `toml:"bind-addr"`
	BindPort int    `toml:"bind-port"`
}

func Start(
	resolver *cluster.Resolver,
	partitioner cluster.Partitioner,
	recovery cluster.RecoveryStorage,
	pks cluster.PartitionKeyStorage,
	ns cluster.NodeStorage,
	config Config) {

	addr := config.BindAddr + ":" + strconv.FormatInt(int64(config.BindPort), 10)

	client := &http.Client{Timeout: 10 * time.Second}
	ch := &ClusterHandler{pks, ns}
	http.Handle("/", &QueryHandler{client, resolver, partitioner, ch})
	http.Handle("/write", &WriteHandler{client, resolver, partitioner, recovery})

	log.Println("Listening on " + addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
