package cluster

import (
	"testing"
	"log"
)

func TestJoin(t *testing.T) {
	local := NewLocalNode()
	log.Print(local.Status)
}
