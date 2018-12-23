package launcher

import (
	"fmt"
	influx "github.com/influxdata/influxdb/client/v2"
	"log"
	"strings"
	"time"
)

func isHttpAvailable(location string) (err error) {
	ticker := time.Tick(8 * time.Second)
	stop := time.After(24 * time.Second)
	client := newInfluxClient(location)
	if checkIsAvailable(client) == nil {
		return nil
	}
	select {
	case <-ticker:
		err = checkIsAvailable(client)
		if err == nil {
			return nil
		}
	case <-stop:
		break
	}
	return err
}

func checkIsAvailable(client influx.Client) error {
	_, version, err := client.Ping(4 * time.Second)
	if err != nil {
		return fmt.Errorf("request error: %s", err.Error())
	}
	if !isVersionSupported(version) {
		return fmt.Errorf("unsupported version: %s", version)
	}
	return nil
}

func isVersionSupported(version string) bool {
	parts := strings.Split(version, ".")
	return parts[0] == "1"
}

func newInfluxClient(location string) influx.Client {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: "http://" + location,
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}
