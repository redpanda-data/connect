package pure

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestLRUCacheStandard(t *testing.T) {
	t.Parallel()

	defConf, err := lruCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := lruMemCacheFromConfig(defConf)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestLRUCacheOptimistic(t *testing.T) {
	t.Parallel()

	defConf, err := lruCacheConfig().ParseYAML(`
optimistic: true
`, nil)
	require.NoError(t, err)

	c, err := lruMemCacheFromConfig(defConf)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestLRUCacheInitValues(t *testing.T) {
	t.Parallel()

	defConf, err := lruCacheConfig().ParseYAML(`
cap: 1024
init_values:
  foo: bar
  foo2: bar2
`, nil)
	require.NoError(t, err)

	c, err := lruMemCacheFromConfig(defConf)
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

func TestLRUCacheAlgorithms(t *testing.T) {
	t.Parallel()

	algorithms := []string{"standard", "arc", "two_queues"}

	for _, algorithm := range algorithms {
		algorithm := algorithm
		t.Run(algorithm, func(t *testing.T) {
			t.Parallel()

			yamlConf := fmt.Sprintf("algorithm: %q", algorithm)

			defConf, err := lruCacheConfig().ParseYAML(yamlConf, nil)
			require.NoError(t, err)

			c, err := lruMemCacheFromConfig(defConf)
			require.NoError(t, err)

			testServiceCache(t, c)
		})
	}
}

func BenchmarkLRU(b *testing.B) {
	defConf, err := lruCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := lruMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % lruCacheFieldCapDefaultValue
		key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkLRUParallel(b *testing.B) {
	defConf, err := lruCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := lruMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for i := 0; p.Next(); i++ {
			j := i % lruCacheFieldCapDefaultValue
			key, value := fmt.Sprintf("key%v", j), []byte(fmt.Sprintf("foo%v", j))

			assert.NoError(b, c.Set(ctx, key, value, nil))

			res, err := c.Get(ctx, key)
			require.NoError(b, err)
			assert.Equal(b, value, res)
		}
	})
}

func testServiceCache(t *testing.T, c service.Cache) {
	t.Helper()

	ctx := context.Background()

	expErr := service.ErrKeyNotFound
	if _, act := c.Get(ctx, "foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err := c.Set(ctx, "foo", []byte("1"), nil); err != nil {
		t.Error(err)
	}

	exp := "1"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err := c.Add(ctx, "bar", []byte("2"), nil); err != nil {
		t.Error(err)
	}

	exp = "2"
	if act, err := c.Get(ctx, "bar"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	expErr = service.ErrKeyAlreadyExists
	if act := c.Add(ctx, "foo", []byte("2"), nil); expErr != act {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err := c.Set(ctx, "foo", []byte("3"), nil); err != nil {
		t.Error(err)
	}

	exp = "3"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err := c.Delete(ctx, "foo"); err != nil {
		t.Error(err)
	}

	if _, err := c.Get(ctx, "foo"); err != service.ErrKeyNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, service.ErrKeyNotFound)
	}
}
