package service

import (
	"github.com/adamringhede/influxdb-ha/cluster"
	"log"
	"net/http"
	"strconv"
	"context"
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
	config Config,
	ctx context.Context) {

	addr := config.BindAddr + ":" + strconv.FormatInt(int64(config.BindPort), 10)

	ch := &ClusterHandler{pks, ns, auth}

	mux := http.NewServeMux()
	mux.Handle("/", NewQueryHandler(resolver, partitioner, ch, auth))
	mux.Handle("/write", NewWriteHandler(resolver, partitioner, auth, NewHttpPointsWriter(recovery)))

	srv := http.Server{Addr: addr, Handler: mux}

	log.Println("Listening on " + addr)
	err := srv.ListenAndServe()
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

	done := ctx.Done()
	select {
		case <-done:
			srv.Close()
	}
}
