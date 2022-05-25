package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIToBytes(t *testing.T) {
	vb := IToBytes(uint64(12345))
	assert.Equal(t, "12345", string(vb))

	vb = IToBytes(true)
	assert.Equal(t, "true", string(vb))

	vb = IToBytes(float64(1.2345))
	assert.Equal(t, "1.2345", string(vb))

	vb = IToBytes(float64(1.234567891234567891234567))
	assert.Equal(t, "1.234567891234568", string(vb))

	vb = IToBytes(float64(1.23 * 4.567 * 1_000_000_000))
	assert.Equal(t, "5.61741e+09", string(vb))
}
