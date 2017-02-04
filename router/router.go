package main

import (
	"net/http"
	"log"
	"io/ioutil"
	"io"
	"bytes"
	"net/url"
)

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
		log.Fatal("route " + target, err)
	}
}

func handle() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		buf, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal("request",err)
		}
		log.Printf("Received request %s?%s\n", r.URL.Path, r.URL.RawQuery)
		if r.URL.Path == "/write" {
			// send to relay
			// if shard is enabled, select the right replica set
			res, err := http.Post("http://192.168.99.100:9096/write?"+r.URL.RawQuery, "image/jpeg", bytes.NewBuffer(buf))
			handleRouteError("write", err)
			passBack(&w, res)
		} else {
			// send to any replica
			baseUrl, _ := url.Parse("http://192.168.99.100:8086" + r.URL.Path)
			baseUrl.RawQuery = r.URL.Query().Encode()
			res, err := http.Get(baseUrl.String())
			handleRouteError("query", err)
			passBack(&w, res)
		}
	}
}

func Start() {
	port := ":5096"
	// get configuration for replica sets
	/*
	[{
		Name: "rs1",
		Relays: [{name: "r1", "host": "hostname:port"}, {name: "r2", "host": "hostname:port"}],
		DataNodes: [{name: "d1", "host": "hostname:port"}, {name: "d2", "host": "hostname:port"}]
	}]
	 */
	http.HandleFunc("/", handle())
	log.Println("Listening on " + port)
	err := http.ListenAndServe(port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func main() {
	Start()
}