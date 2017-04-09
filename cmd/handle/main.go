package main

import (
	"flag"
	"github.com/adamringhede/influxdb-ha/cluster"
	"log"
	"os"
	"strings"
)

func main() {

	bindAddr := flag.String("addr", "0.0.0.0", "IP addres for listening on cluster communication")
	bindPort := flag.Int("port", 8084, "Port for listening on cluster communication")
	join := flag.String("join", "", "Comma seperated locations of other nodes")
	flag.Parse()

	config := cluster.Config{
		BindAddr: *bindAddr,
		BindPort: *bindPort,
	}

	hostname, nameErr := os.Hostname()
	if nameErr != nil {
		panic(nameErr)
	}
	log.Printf("Hostname: %s", hostname)

	handle, err := cluster.NewHandle(config)

	if err != nil {
		panic(err)
	}

	others := strings.Split(*join, ",")
	if len(others) > 0 && *join != "" {
		log.Printf("Joining: %s", *join)
		joinErr := handle.Join(others)
		if joinErr != nil {
			log.Println("Failed to join any other node")
		}
	}

	ch := make(chan int)
	<-ch

}
