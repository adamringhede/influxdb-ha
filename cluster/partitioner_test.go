package cluster

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"sort"
)

func Test_createCompoundKeys(t *testing.T) {
	values := map[string][]string{
		"type": {"gold", "silver"},
		"captain": {"goblin", "pirate"},
	}
	key := PartitionKey{"sharded", "treasures", []string{"type", "captain"}}
	result := createCompoundKeys(key, values)
	assert.NotEmpty(t, result)
	sort.Strings(result)
	assert.Equal(t, "goldgoblin", result[0])
	assert.Equal(t, "goldpirate", result[1])
	assert.Equal(t, "silvergoblin", result[2])
	assert.Equal(t, "silverpirate", result[3])
}
