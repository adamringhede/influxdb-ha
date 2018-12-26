package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/adamringhede/influxdb-ha/syncing"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	host := flag.String("host", "", "The host and port for the source deployment (1.2.3.4:8086)")
	username := flag.String("username", "", "Username for user with all privileges on the source deployment")
	password := flag.String("password", "", "Password for the user to the source deployment")

	destination := flag.String("destination", "", "The host and port of the target deployment for one of the nodes or a load-balancer. (1.2.3.5:8086)")
	destinationUsername := flag.String("destination-username", "", "Username for user on the destination deployment with all privileges.")
	destinationPassword := flag.String("destination-password", "", "Password for the user on the destination deployment")

	bookmarkFilePath := flag.String("bookmark-file", "./influx-mirror-bookmark.json", "File path where a bookmark is stored to be able to resume the sync.")

	flag.Parse()

	validationErrors := []error{
		validateLocation("-host", *host),
		validateLocation("-destination", *destination),
	}
	hasError := false
	for _, err := range validationErrors {
		if err != nil {
			println(err.Error())
			hasError = true
		}
	}
	if hasError {
		os.Exit(1)
	}

	hostClient, err := syncing.NewInfluxClientHTTP(*host, *username, *password)
	if err != nil {
		panic(err)
	}

	destinationClient, err := syncing.NewInfluxClientHTTP(*destination, *destinationUsername, *destinationPassword)
	if err != nil {
		panic(err)
	}

	saver := NewFileSystemBookmarkSaver(*bookmarkFilePath)

	bookmark := NewBookmark(func(bookmark Bookmark) {
		err := saver.Save(bookmark)
		if err != nil {
			log.Panicf("Failed to save bookmark: %s", err.Error())
		}
	})

	importer := syncing.NewInfluxImporter()
	importer.ImportAll(hostClient, destinationClient, bookmark)

	err = saver.Cleanup()
	if err != nil {
		fmt.Printf("Warning: Could not cleanup bookmark file: %s", err.Error())
	}
}

func validateLocation(param string, value string) error {
	if value == "" {
		return fmt.Errorf("missing required option %s", param)
	}
	return nil
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
	OnUpdate  func(bookmark Bookmark) `json:"-"`
}

func NewBookmark(onUpdate func(bookmark Bookmark)) *Bookmark {
	return &Bookmark{Databases: map[string]DatabaseBookmark{}, OnUpdate: onUpdate}
}

func (bookmark *Bookmark) Set(db, rp, measurement string, timestamp int, done bool) {
	if _, ok := bookmark.Databases[db]; !ok {
		bookmark.Databases[db] = DatabaseBookmark{map[string]RetentionPolicyBookmark{}}
	}
	if _, ok := bookmark.Databases[db].RetentionPolicies[rp]; !ok {
		bookmark.Databases[db].RetentionPolicies[rp] = RetentionPolicyBookmark{map[string]MeasurementBookmark{}}
	}
	bookmark.Databases[db].RetentionPolicies[rp].Measurements[measurement] = MeasurementBookmark{
		Timestamp: timestamp,
		Done:      done,
	}
	bookmark.OnUpdate(*bookmark)
}

// Get returns the timestamp of the bookmark, if it is finished, and if the bookmark is set in that order.
func (bookmark *Bookmark) Get(db, rp, measurement string) (int, bool, bool) {
	if _, ok := bookmark.Databases[db]; ok {
		if _, ok := bookmark.Databases[db].RetentionPolicies[rp]; ok {
			if status, ok := bookmark.Databases[db].RetentionPolicies[rp].Measurements[measurement]; ok {
				return status.Timestamp, status.Done, true
			}
		}
	}
	return -1, false, false
}

type BookmarkSaver interface {
	Save(bookmark Bookmark) error
}

type FileSystemBookmarkSaver struct {
	FilePath string
}

func NewFileSystemBookmarkSaver(filePath string) *FileSystemBookmarkSaver {
	return &FileSystemBookmarkSaver{FilePath: filePath}
}

func (saver *FileSystemBookmarkSaver) Save(bookmark Bookmark) error {
	data, err := json.Marshal(bookmark)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(saver.FilePath, data, 0644)
}

func (saver *FileSystemBookmarkSaver) Cleanup() error {
	return os.Remove(saver.FilePath)
}
