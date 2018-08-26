package service

import (
	"errors"
	"fmt"
	"github.com/adamringhede/influxdb-ha/cluster"
	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouting(t *testing.T) {
	pointsWriter := NewMockPointsWriter()

	handler, _, _, _ := newTestWriteHandler(pointsWriter)

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

func BenchmarkRouting(b *testing.B) {
	pointsWriter := NewMockPointsWriter()

	handler, _, _, _ := newTestWriteHandler(pointsWriter)
	linesBuilder := strings.Builder{}
	for i := 0; i < 100; i++ {
		linesBuilder.WriteString("asdf,type=gold value=29 1439856000\n")
	}
	payload := linesBuilder.String()
	for i := 0; i < b.N; i++ {
		writeUrl := fmt.Sprintf("http://localhost/write?db=%s", testDB)
		req := httptest.NewRequest("POST", writeUrl, strings.NewReader(payload))
		req.SetBasicAuth("admin", "secret")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		res, err := ioutil.ReadAll(w.Body)
		assert.NoError(b, err)
		println(string(res))
	}
}

func newTestWriteHandler(pointsWriter PointsWriter) (*WriteHandler, *cluster.Resolver, cluster.Partitioner, AuthService) {
	authStorage := cluster.NewMockAuthStorage()
	authService := NewPersistentAuthService(authStorage)

	authService.CreateUser(cluster.UserInfo{Name: "admin", Hash: cluster.HashUserPassword("secret"), Admin: true})
	authService.Save()

	resolver := newTestResolver()
	partitioner := newPartitioner()
	handler := NewWriteHandler(resolver, partitioner, authService, pointsWriter)
	return handler, resolver, partitioner, authService
}

/*
TODO Test saving to recovery with mocked recovery storage
	This requires moving the recovery out of the HttpPointsWriter
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

type MockRecoveryStorage struct {
	data map[string]bool
}

func NewMockRecoveryStorage() *MockRecoveryStorage {
	return &MockRecoveryStorage{map[string]bool{}}
}

func (rs *MockRecoveryStorage) Put(nodeName, db, rp string, buf []byte) error {
	rs.data[strings.Join([]string{nodeName, db, rp}, ".")] = true
	return nil
}

func (rs *MockRecoveryStorage) Get(nodeName string) (chan cluster.RecoveryChunk, error) {
	ch := make(chan cluster.RecoveryChunk)
	return ch, nil
}

func (rs *MockRecoveryStorage) Drop(nodeName string) error {
	return nil
}

func (rs *MockRecoveryStorage) hasData() bool {
	return len(rs.data) > 0
}