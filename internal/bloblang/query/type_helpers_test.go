package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestIToInt(t *testing.T) {
	for _, test := range []struct {
		in          any
		out         int64
		errContains string
	}{
		{
			in:  123.0,
			out: 123,
		},
		{
			in:          123.456,
			errContains: "contains decimals and therefore cannot be cast as a",
		},
		{
			in:  "123",
			out: 123,
		},
		{
			in:  MaxInt,
			out: int64(MaxInt),
		},
		{
			in:  MinInt,
			out: MinInt,
		},
		{
			in:          float64(MaxInt) + 10000,
			errContains: "value is too large to be cast as a",
		},
		{
			in:          float64(MinInt) - 10000,
			errContains: "value is too small to be cast as a",
		},
	} {
		i, err := IToInt(test.in)
		if test.errContains != "" {
			require.Error(t, err, "value: %v", i)
			assert.Contains(t, err.Error(), test.errContains)
		} else {
			require.NoError(t, err)
			assert.Equal(t, test.out, i)
		}
	}
}
