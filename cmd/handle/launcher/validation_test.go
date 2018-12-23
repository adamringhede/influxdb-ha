package launcher

import (
	"github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type influxMock struct{}

func (influxMock) Ping(timeout time.Duration) (time.Duration, string, error) {
	return time.Millisecond, "1.2.3", nil
}

func (influxMock) Write(bp client.BatchPoints) error {
	panic("implement me")
}

func (influxMock) Query(q client.Query) (*client.Response, error) {
	panic("implement me")
}

func (influxMock) QueryAsChunk(q client.Query) (*client.ChunkedResponse, error) {
	panic("implement me")
}

func (influxMock) Close() error {
	panic("implement me")
}

func Test_checkIsAvailable(t *testing.T) {
	err := checkIsAvailable(influxMock{})
	assert.NoError(t, err)
}