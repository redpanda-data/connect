package pure

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTTLRUCacheDefault(t *testing.T) {
	t.Parallel()

	defConf, err := ttlruCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := ttlruMemCacheFromConfig(defConf)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestTTLRUCacheOptimisticAndWithoutReset(t *testing.T) {
	t.Parallel()

	defConf, err := ttlruCacheConfig().ParseYAML(`
optimistic: true
without_reset: true
`, nil)
	require.NoError(t, err)

	c, err := ttlruMemCacheFromConfig(defConf)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestTTLRUCacheInitValues(t *testing.T) {
	t.Parallel()

	defConf, err := ttlruCacheConfig().ParseYAML(`
ttl: '5m'
cap: 1024
init_values:
  foo: bar
  foo2: bar2
`, nil)
	require.NoError(t, err)

	c, err := ttlruMemCacheFromConfig(defConf)
	require.NoError(t, err)

	ctx := context.Background()

	exp := "bar"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	exp = "bar2"
	if act, err := c.Get(ctx, "foo2"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}
}

func BenchmarkTTLRU(b *testing.B) {
	defConf, err := ttlruCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := ttlruMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % ttlruCacheFieldCapDefaultValue
		key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkTTLRUParallel(b *testing.B) {
	defConf, err := ttlruCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := ttlruMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for i := 0; p.Next(); i++ {
			j := i % ttlruCacheFieldCapDefaultValue
			key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

			assert.NoError(b, c.Set(ctx, key, value, nil))

			res, err := c.Get(ctx, key)
			require.NoError(b, err)
			assert.Equal(b, value, res)
		}
	})
}
