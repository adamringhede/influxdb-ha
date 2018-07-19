package service

import (
		"testing"
	"time"
					"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/coreos/etcd/clientv3"
	"net/http/httptest"
	"fmt"
	"strings"
		"github.com/stretchr/testify/assert"
)

func TestRouting(t *testing.T) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	authStorage := cluster.NewMockAuthStorage()
	authService := NewPersistentAuthService(authStorage)

	authService.CreateUser(cluster.UserInfo{Name:"admin", Hash: cluster.HashUserPassword("secret"), Admin: true})
	authService.Save()

	hintsStorage := cluster.NewEtcdHintStorage(c, "test")
	recoveryStorage := cluster.NewLocalRecoveryStorage("./", hintsStorage)
	resolver := newTestResolver()
	partitioner := newPartitioner()
	handler := NewWriteHandler(resolver, partitioner, recoveryStorage, authService)

	line := "treasures,type=gold value=29 1439856000"
	writeUrl := fmt.Sprintf("http://localhost/write?db=%s", testDB)
	req := httptest.NewRequest("GET", writeUrl, strings.NewReader(line))
	req.SetBasicAuth("admin", "secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 204)

	queryHandler := NewQueryHandler(resolver, partitioner, nil, authService)
	mustQueryClusterAuth(t, queryHandler, "CREATE DATABASE " + testDB, "admin:secret")
	result := mustQueryClusterAuth(t, queryHandler, "SELECT * FROM treasures WHERE type='gold'", "admin:secret")
	assert.Len(t, result, 1)
	assert.Len(t, result[0].Series[0].Values, 1)

}

/*
TODO Find a way of mocking the data nodes
TODO Separate testing the partitioned writing and the HTTP service.
TODO Test hinted hand off
TODO Test retries
TODO Test different configurations

 */