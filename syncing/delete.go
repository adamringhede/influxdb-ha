package syncing

import (
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/adamringhede/influxdb-ha/cluster"
)

func Delete(token int, location string) {
	client := &http.Client{}
	meta, err := fetchLocationMeta(location)
	if err != nil {
		log.Printf("Failed fetching meta from location %s. Error: %s", location, err.Error())
		return
	}
	for db, dbMeta := range meta.databases {
		for _, rp := range dbMeta.rps {
			q := `DROP SERIES WHERE ` + cluster.PartitionTagName + `='` + strconv.Itoa(token) + `'`
			log.Printf("%s: %s", location, q)
			params := []string{"db=" + db, "q=" + q, "rp=" + rp}
			values, err := url.ParseQuery(strings.Join(params, "&"))
			if err != nil {
				log.Panic(err)
			}
			client.Get("http://" + location + "/query?" + values.Encode())
		}
	}
}
