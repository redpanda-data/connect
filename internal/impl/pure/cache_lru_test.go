package pure

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/benthosdev/benthos/v4/public/service/middleware"
)

func TestLRUCacheStandard(t *testing.T) {
	t.Parallel()

	defConf, err := lruCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := lruMemCacheFromConfig(defConf, nil)
	require.NoError(t, err)

	testServiceCache(t, c)
}

func TestLRUCacheOptimistic(t *testing.T) {
	t.Parallel()

	defConf, err := lruCacheConfig().ParseYAML(`
optimistic: true
`, nil)
	require.NoError(t, err)

	c, err := lruMemCacheFromConfig(defConf, nil)
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

	var ctor service.CacheConstructor = lruMemCacheFromConfig

	ctor = middleware.WrapCacheInitValues(context.Background(), ctor)

	c, err := ctor(defConf, nil)
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

			c, err := lruMemCacheFromConfig(defConf, nil)
			require.NoError(t, err)

			testServiceCache(t, c)
		})
	}
}

func BenchmarkLRU(b *testing.B) {
	defConf, err := lruCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	c, err := lruMemCacheFromConfig(defConf, nil)
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

	c, err := lruMemCacheFromConfig(defConf, nil)
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
	key := "foo"

	expErr := service.ErrKeyNotFound

	if _, act := c.Get(ctx, key); !errors.Is(act, expErr) {
		t.Errorf("wrong error returned on c.Get(ctx, %q): %v != %v", key, act, expErr)
	}

	if err := c.Set(ctx, key, []byte("1"), nil); err != nil {
		t.Errorf("unexpected error while c.Set(ctx, %q, <data>): %v", key, err)
	}

	exp := "1"
	if act, err := c.Get(ctx, key); err != nil {
		t.Errorf("unexpected error while c.Get(ctx, %q): %v", key, err)
	} else if string(act) != exp {
		t.Errorf("Wrong result c.Get(ctx, %q): %v != %v", key, string(act), exp)
	}

	key = "bar"

	if err := c.Add(ctx, key, []byte("2"), nil); err != nil {
		t.Errorf("unexpected error while c.Add(ctx, %q, <data>): %v", key, err)
	}

	exp = "2"

	if act, err := c.Get(ctx, key); err != nil {
		t.Errorf("unexpected error while c.Get(ctx, %q): %v", key, err)
	} else if string(act) != exp {
		t.Errorf("wrong result c.Get(ctx, %q): %v != %v", key, string(act), exp)
	}

	key = "foo"
	expErr = service.ErrKeyAlreadyExists

	if act := c.Add(ctx, key, []byte("2"), nil); !errors.Is(act, expErr) {
		t.Errorf("unexpected error returned on c.Add(ctx, %q, <data>): %v != %v", key, act, expErr)
	}

	if err := c.Set(ctx, key, []byte("3"), nil); err != nil {
		t.Errorf("unexpected error while c.Set(ctx, %q, <data>): %v", key, err)
	}

	exp = "3"
	if act, err := c.Get(ctx, key); err != nil {
		t.Errorf("unexpected error while c.Get(ctx, %q): %v", key, err)
	} else if string(act) != exp {
		t.Errorf("wrong result c.Get(ctx, %q): %v != %v", key, string(act), exp)
	}

	if err := c.Delete(ctx, key); err != nil {
		t.Errorf("unexpected error while c.Delete(ctx, %q, <data>): %v", key, err)
	}

	expErr = service.ErrKeyNotFound

	if _, act := c.Get(ctx, key); !errors.Is(act, expErr) {
		t.Errorf("wrong error returned on c.Get(ctx, %q): %v != %v", key, act, expErr)
	}
}
