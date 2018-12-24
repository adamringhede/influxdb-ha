package main

import (
	"flag"
	"github.com/adamringhede/influxdb-ha/syncing"
)

func main() {
	// 1. collect meta data
	// 2. create a subscription to start get the latest data
	// 3. start backfilling data

	flag.String("host", "", "The host and port for the source deployment")
	flag.String("username", "", "Username for user with all privileges on the source deployment")
	flag.String("password", "", "Password for the user to the source deployment")

	flag.String("destination", "", "The host and port of the target deployment for one of the nodes or a load-balancer.")
	flag.String("destination-username", "", "Username for user on the destination deployment with all privileges.")
	flag.String("destination-password", "", "Password for the user on the destination deployment")

	flag.String("bookmark-file", "./influx-mirror-bookmark.json", "File path where a bookmark is stored to be able to resume the sync.")

	// TODO maybe also have options for what databases and retention policies to select

	flag.Parse()


	importer := syncing.NewInfluxImporter()

}

type DatabaseBookmark struct {
	RetentionPolicies map[string]RetentionPolicyBookmark
}

type RetentionPolicyBookmark struct {
	// Measurements has timestamps in nanoseconds of the last imported value
	Measurements map[string]MeasurementBookmark
}

type MeasurementBookmark struct {
	Timestamp int
	Done      bool
}

type Bookmark struct {
	Databases map[string]DatabaseBookmark
}

func (bookmark *Bookmark) Set(db, rp, measurement string, timestamp int, done bool) {
	if _, ok := bookmark.Databases[db]; !ok {
		bookmark.Databases[db] = DatabaseBookmark{}
	}
	if _, ok := bookmark.Databases[db].RetentionPolicies[rp]; !ok {
		bookmark.Databases[db].RetentionPolicies[rp] = RetentionPolicyBookmark{}
	}
	bookmark.Databases[db].RetentionPolicies[rp].Measurements[measurement] = MeasurementBookmark{
		Timestamp: timestamp,
		Done: done,
	}
}

type BookmarkSaver interface {
	Save(bookmark Bookmark) error
}

type Configuration struct {
	host                string
	username            string
	password            string
	destination         string
	destinationUsername string
	destinationPassword string
}

type InitialSyncer interface {
}
