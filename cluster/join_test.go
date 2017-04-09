package cluster

import (
	"log"
	"testing"
)

func TestJoin(t *testing.T) {
	local := NewLocalNode()
	log.Print(local.Status)
}
