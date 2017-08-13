package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestNewLocalSaver(t *testing.T) {
	s := NewLocalRecoveryStorage("./", nil)
	defer s.Drop("node1")
	defer s.Close()
	err := s.Put("node1", "mydb", "default", []byte("test"))
	assert.NoError(t, err)
	time.Sleep(1)
	ch, err := s.Get("node1")
	assert.NoError(t, err)
	res := <- ch
	assert.Equal(t, "mydb", res.DB)
	assert.Equal(t, "default", res.RP)
	assert.Equal(t, "test", string(res.Buf))
}