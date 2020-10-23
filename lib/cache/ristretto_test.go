package cache

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRistrettoCache(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeRistretto
	conf.Ristretto.Retries = 50
	conf.Ristretto.RetryPeriod = "1ms"

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = c.Get("foo")
	assert.Equal(t, types.ErrKeyNotFound, err)

	require.NoError(t, c.Set("foo", []byte("1")))

	res, err := c.Get("foo")

	require.NoError(t, err)
	assert.Equal(t, []byte("1"), res)

	assert.NoError(t, c.Delete("foo"))

	_, err = c.Get("foo")
	assert.Equal(t, types.ErrKeyNotFound, err)
}
