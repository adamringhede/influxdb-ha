package main

import (
	"github.com/adamringhede/influxdb-ha/router"
	"flag"
	"log"
)

func main() {
	// if given a config file filename, parse the file and give
	// the configuration to the router.
	configFileName := flag.String("config", "", "Path to configuration file")
	//routerName := flag.String("name", "", "Name of the router. Should be same as in config file.")
	flag.Parse()

	config, err := router.ParseConfigFile(*configFileName)
	if err != nil {
		log.Panic(err)
	}
/*
	if len(config.Routers) == 0 {
		log.Panic(errors.New("Configuration requires at least one router."))
	}

	var routerConfig *router.RouterConfig
	if *routerName != "" {
		for _, r := range config.Routers {
			if r.Name == *routerName {
				routerConfig = &r
				break
			}
		}
		if routerConfig == nil {
			log.Panic("Could not find router with name " + *routerName)
		}
	} else {
		routerConfig = &config.Routers[0]
	}*/
	routerConfig := &config.Routers[0]
	log.Printf("Starting router: %s", routerConfig.Name)

	router.Start(routerConfig, config.ReplicaSets)
}
