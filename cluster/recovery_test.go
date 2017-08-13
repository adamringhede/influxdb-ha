package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestNewLocalSaver(t *testing.T) {
	s := NewLocalStorage("./", nil)
	defer s.Drop("node1")
	defer s.Close()
	err := s.Put("node1", []byte("test"))
	assert.NoError(t, err)
	time.Sleep(1)
	ch, err := s.Get("node1")
	assert.NoError(t, err)
	res := <- ch
	assert.Equal(t, "test", string(res))
}