package cluster

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const testFileName = "./node_state"

func TestBoltStorage(t *testing.T) {
	defer os.Remove(testFileName)

	storage, openErr := openBoltStorage(testFileName)
	assert.NoError(t, openErr)

	saveErr := storage.save(persistentState{[]int{1, 2, 3}})
	assert.NoError(t, saveErr)

	state, getErr := storage.get()
	assert.NoError(t, getErr)
	assert.Equal(t, []int{1, 2, 3}, state.Tokens)
}
