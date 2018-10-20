package clusterql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser_ParseShow(t *testing.T) {
	lang := CreateLanguage()

	stmt, err := NewParser(strings.NewReader(`SHOW PARTITION KEYS ON "mydb"`), lang).Parse()
	assert.NoError(t, err)
	assert.IsType(t, ShowPartitionKeysStatement{}, stmt)
	assert.Equal(t, "mydb", stmt.(ShowPartitionKeysStatement).Database)
}

func TestParserErrorOnMissingParameter(t *testing.T) {
	lang := CreateLanguage()

	stmt, err := NewParser(strings.NewReader(`create partition key on consumption`), lang).Parse()
	assert.Nil(t, stmt)
	assert.Equal(t, "unexpected end of statement, expecting WITH", err.Error())
}