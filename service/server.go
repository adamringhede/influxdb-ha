package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"log"
	"net/http"
	"strconv"
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
	auth AuthService,
	config Config) {

	addr := config.BindAddr + ":" + strconv.FormatInt(int64(config.BindPort), 10)

	ch := &ClusterHandler{pks, ns, auth}

	http.Handle("/", NewQueryHandler(resolver, partitioner, ch, auth))
	http.Handle("/write", NewWriteHandler(resolver, partitioner, auth, NewHttpPointsWriter(recovery)))

	log.Println("Listening on " + addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
