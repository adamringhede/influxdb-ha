package cluster

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestCreateNodeWithStorage(t *testing.T) {
	defer os.Remove(testFileName)

	storage, openErr := openBoltStorage(testFileName)
	assert.NoError(t, openErr)

	node := CreateNodeWithStorage(storage)
	assert.NoError(t, node.Init())
	assert.Len(t, node.Tokens, 256)
}
