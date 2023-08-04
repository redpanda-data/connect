package pure

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCuckooCacheStandard(t *testing.T) {
	t.Parallel()

	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := cuckooMemCacheFromConfig(defConf)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestCuckooCacheInitValues(t *testing.T) {
	t.Parallel()

	defConf, err := cuckooCacheConfig().ParseYAML(`
cap: 1024
init_values:
  - foo
  - bar
`, nil)
	require.NoError(t, err)

	c, err := cuckooMemCacheFromConfig(defConf)
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

func TestCuckooCacheAlgorithm(t *testing.T) {
	t.Parallel()

	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := cuckooMemCacheFromConfig(defConf)
	require.NoError(t, err)

	// TODO add unit test
	_ = c
}

func BenchmarkCuckoo(b *testing.B) {
	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := cuckooMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % cuckooCacheFieldCapDefaultValue
		key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkCuckooParallel(b *testing.B) {
	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := cuckooMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for i := 0; p.Next(); i++ {
			j := i % cuckooCacheFieldCapDefaultValue
			key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

			assert.NoError(b, c.Set(ctx, key, value, nil))

			res, err := c.Get(ctx, key)
			require.NoError(b, err)
			assert.Equal(b, value, res)
		}
	})
}
