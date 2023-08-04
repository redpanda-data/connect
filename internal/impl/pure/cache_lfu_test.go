package pure

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLFUCacheStandard(t *testing.T) {
	t.Parallel()

	defConf, err := lfuCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := lfuMemCacheFromConfig(defConf)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestLFUCacheInitValues(t *testing.T) {
	t.Parallel()

	defConf, err := lfuCacheConfig().ParseYAML(`
cap: 1024
samples: 99999
init_values:
  foo: bar
  foo2: bar2
`, nil)
	require.NoError(t, err)

	c, err := lfuMemCacheFromConfig(defConf)
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

func BenchmarkLFU(b *testing.B) {
	defConf, err := lfuCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := lfuMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % lfuCacheFieldSizeDefaultValue
		key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkLFUParallel(b *testing.B) {
	defConf, err := lfuCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := lfuMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for i := 0; p.Next(); i++ {
			j := i % lfuCacheFieldSizeDefaultValue
			key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

			assert.NoError(b, c.Set(ctx, key, value, nil))

			res, err := c.Get(ctx, key)
			require.NoError(b, err)
			assert.Equal(b, value, res)
		}
	})
}
