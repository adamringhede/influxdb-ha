package service

import (
	"errors"
	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRouting(t *testing.T) {
	handler, _, _, _ := newTestWriteHandler()
	pointsWriter := NewMockPointsWriter()
	handler.pointsWriter = pointsWriter

	line := `asdf,type=gold value=29 1439856000
             asdf,type=gold value=29 1439859000`
	writeUrl := fmt.Sprintf("http://localhost/write?db=%s", testDB)
	req := httptest.NewRequest("GET", writeUrl, strings.NewReader(line))
	req.SetBasicAuth("admin", "secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 204)

	assert.Equal(t, 1, pointsWriter.numNodesWithPoints())

	/* For testing without a mocked points writer
	queryHandler := NewQueryHandler(resolver, partitioner, nil, authService)
	result := mustQueryClusterAuth(t, queryHandler, "SELECT * FROM asdf WHERE type='gold'", "admin:secret")
	assert.Len(t, result, 1)
	assert.Len(t, result[0].Series[0].Values, 2)*/
}

func newTestWriteHandler() (*WriteHandler, *cluster.Resolver, cluster.Partitioner, AuthService) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	authStorage := cluster.NewMockAuthStorage()
	authService := NewPersistentAuthService(authStorage)

	authService.CreateUser(cluster.UserInfo{Name: "admin", Hash: cluster.HashUserPassword("secret"), Admin: true})
	authService.Save()

	hintsStorage := cluster.NewEtcdHintStorage(c, "test")
	recoveryStorage := cluster.NewLocalRecoveryStorage("./", hintsStorage)
	resolver := newTestResolver()
	partitioner := newPartitioner()
	handler := NewWriteHandler(resolver, partitioner, recoveryStorage, authService)
	return handler, resolver, partitioner, authService
}

/*
TODO Test saving to recovery with mocked recovery storage (this will remove the need for hints storage and etcd)
*/

type MockPointsWriter struct {
	writtenPoints map[string][]models.Point
}

func NewMockPointsWriter() *MockPointsWriter {
	return &MockPointsWriter{map[string][]models.Point{}}
}

func (w *MockPointsWriter) WritePoints(points []models.Point, locations []*cluster.Node, writeContext WriteContext) error {
	for _, node := range locations {
		if _, ok := w.writtenPoints[node.Name]; !ok {
			w.writtenPoints[node.Name] = []models.Point{}
		}
		w.writtenPoints[node.Name] = append(w.writtenPoints[node.Name], points...)
	}
	return nil
}

func (w *MockPointsWriter) Reset() {
	w.writtenPoints = map[string][]models.Point{}
}

func (w *MockPointsWriter) numNodesWithPoints() int {
	return len(w.writtenPoints)
}

type FailingPointsWriter struct{}

func (w *FailingPointsWriter) WritePoints(points []models.Point, locations []*cluster.Node, writeContext WriteContext) error {
	return errors.New("write failed")
}
