package cluster

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_compareIntSlices(t *testing.T) {
	a := []int{1, 2}
	b := []int{2, 3}
	adiff, bdiff := compareIntSlices(a, b)
	assert.Contains(t, adiff, 1)
	assert.Contains(t, bdiff, 3)
}
