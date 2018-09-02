package cluster

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/influxdata/influxdb/models"
		"github.com/influxdata/influxql"
	"golang.org/x/crypto/bcrypt"
	"log"
	)

/*

This file will contain code for storing authentication and authorization logic.
Hopefully we can even reuse a bunch of code from influx to make this implementation smoother.

This is also an opportunity to implement access control features that are not part of the
open source offering. However, the gaol should be to be as similar to InfluxDB as possible.

*/

const etcdStorageAuth = "auth"

type UserInfo struct {
	// User's name.
	Name string

	// Hashed password.
	Hash string

	// Whether the user is an admin, i.e. allowed to do everything.
	Admin bool

	// Map of database name to granted privilege.
	Privileges map[string]influxql.Privilege
}

// AuthorizeDatabase returns true if the user is authorized for the given privilege on the given database.
func (ui *UserInfo) AuthorizeDatabase(privilege influxql.Privilege, database string) bool {
	if ui.Admin || privilege == influxql.NoPrivileges {
		return true
	}
	p, ok := ui.Privileges[database]
	return ok && (p == privilege || p == influxql.AllPrivileges)
}

func (u *UserInfo) AuthorizeClusterOperation() bool {
	return u == nil || u.Admin
}

func NewUser(name, hash string, admin bool) UserInfo {
	return UserInfo{Name: name, Admin: admin, Hash: hash, Privileges: map[string]influxql.Privilege{}}
}

func HashUserPassword(password string) string {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		panic(err)
	}
	return string(hash)
}

// AuthorizeSeriesRead is used to limit access per-series (enterprise only)
func (u *UserInfo) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite is used to limit access per-series (enterprise only)
func (u *UserInfo) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

type AuthData struct {
	// TODO add a version key to validate that nothing has changed
	Users []UserInfo
}

func NewAuthData() *AuthData {
	return &AuthData{Users: []UserInfo{}}
}

type AuthStorage interface {
	Get() (*AuthData, error)
	Save(*AuthData) error
	Delete() error
	Watch() chan AuthData
}

type EtcdAuthStorage struct {
	EtcdStorageBase
}

type MockAuthStorage struct {
	data *AuthData
}

func NewMockAuthStorage() *MockAuthStorage {
	return &MockAuthStorage{&AuthData{}}
}

func (s *MockAuthStorage) Get() (*AuthData, error) {
	return s.data, nil
}

func (s *MockAuthStorage) Save(data *AuthData) error {
	s.data = data
	return nil
}

func (s *MockAuthStorage) Delete() error {
	return nil
}

func (s *MockAuthStorage) Watch() (out chan AuthData) {
	return
}

func NewEtcdAuthStorage(c *clientv3.Client) *EtcdAuthStorage {
	s := &EtcdAuthStorage{}
	s.Client = c
	return s
}

func (s *EtcdAuthStorage) Get() (*AuthData, error) {
	resp, err := s.Client.Get(context.Background(), s.path(etcdStorageAuth))
	if err != nil {
		return nil, err
	}
	if resp.Count > 0 {
		var auth AuthData
		err = json.Unmarshal(resp.Kvs[0].Value, &auth)
		return &auth, err
	}
	return nil, nil
}

func (s *EtcdAuthStorage) Save(auth *AuthData) error {
	data, err := json.Marshal(auth)
	if err != nil {
		return err
	}
	_, err = s.Client.Put(context.Background(), s.path(etcdStorageAuth), string(data))
	return err
}

func (s *EtcdAuthStorage) Delete() error {
	_, err := s.Client.Delete(context.Background(), s.path(etcdStorageAuth))
	return err
}

func (s *EtcdAuthStorage) Watch() (chan AuthData) {
	out := make(chan AuthData)
	go (func() {
		watch := s.Client.Watch(context.Background(), s.path(etcdStorageAuth))
		for update := range watch {
			for _, event := range update.Events {
				var auth AuthData
				err := json.Unmarshal(event.Kv.Value, &auth)
				if err != nil {
					log.Printf("Failed to parse %s", event.Kv.Key)
					continue
				}
				if event.Type == mvccpb.PUT {
					out <- auth
				}
			}
		}
	})()
	return out
}
