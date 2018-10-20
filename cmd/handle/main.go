package main

import (
	"flag"
	. "github.com/adamringhede/influxdb-ha/cmd/handle/launcher"
	"github.com/adamringhede/influxdb-ha/service"
	"os"
)

func main() {
	hostName, hostErr := os.Hostname()
	if hostErr != nil {
		panic(hostErr)
	}

	bindClientAddr := flag.String("client-addr", "0.0.0.0", "IP address for client http requests")
	bindClientPort := flag.Int("client-port", 80861, "Port for http requests")
	dataLocation := flag.String("data", "localhost:8086", "InfluxDB database public host:port")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "Comma separated locations of etcd nodes")
	clusterID := flag.String("cluster-id", "default", "Comma separated locations of etcd nodes")
	nodeName := flag.String("node-name", hostName, "A unique name of the node to use instead of the hostname")

	flag.Parse()

	httpConfig := service.Config{
		BindAddr: *bindClientAddr,
		BindPort: *bindClientPort,
	}
	Start(*clusterID, *nodeName, *etcdEndpoints, *dataLocation, httpConfig)
}
