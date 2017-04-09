package cluster

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"path/filepath"
)

const bucketName = "influxdbClusterNodeStorage"

type boltStorage struct {
	db *bolt.DB
}

func (s *boltStorage) save(state persistentState) error {
	stateJson, jsonErr := json.Marshal(state)
	if jsonErr != nil {
		return jsonErr
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		err := b.Put([]byte("state"), stateJson)
		return err
	})
	return err
}

func (s *boltStorage) get() (persistentState, error) {
	var state persistentState
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		b.Get([]byte("state"))
		json.Unmarshal(b.Get([]byte("state")), &state)
		return nil
	})
	return state, err
}

func openBoltStorage(filename string) (*boltStorage, error) {
	// Ensure path to file exists
	os.MkdirAll(filepath.Dir(filename), os.ModePerm)

	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return &boltStorage{}, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	return &boltStorage{db}, err
}
