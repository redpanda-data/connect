package pure

import (
	"context"
	"fmt"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloomCacheStandard(t *testing.T) {
	t.Parallel()

	defConf, err := bloomCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	ctx := context.Background()
	key := "foo"

	_, err = c.Get(ctx, key)
	assert.EqualError(t, err, "key does not exist")

	err = c.Add(ctx, key, nil, nil)
	assert.NoError(t, err)

	err = c.Add(ctx, key, nil, nil)
	assert.EqualError(t, err, "key already exists")

	err = c.Set(ctx, key, nil, nil)
	assert.NoError(t, err)

	value, err := c.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, string(value), "t")
}

func TestBloomCacheDelete(t *testing.T) {
	t.Parallel()

	defConf, err := bloomCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	ctx := context.Background()
	key := "foo"

	err = c.Delete(ctx, key)
	assert.EqualError(t, err, "unable to delete key into bloom filter: not supported")
}

func TestBloomCacheInitValues(t *testing.T) {
	t.Parallel()

	defConf, err := bloomCacheConfig().ParseYAML(`
cap: 1024
init_values:
  - foo
  - bar
`, nil)
	require.NoError(t, err)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	ctx := context.Background()

	exp := "t"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if act, err := c.Get(ctx, "bar"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}
}

func BenchmarkBloom(b *testing.B) {
	defConf, err := bloomCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % bloomCacheFieldCapDefaultValue
		key, value := fmt.Sprintf("key%v", j), []byte("t")

		err = c.Set(ctx, key, value, nil)
	}

	_ = err
}

func BenchmarkBloomParallel(b *testing.B) {
	defConf, err := bloomCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var err error

		for i := 0; p.Next(); i++ {
			j := i % bloomCacheFieldCapDefaultValue
			key, value := fmt.Sprintf("key%v", j), []byte("t")

			err = c.Set(ctx, key, value, nil)
		}

		_ = err
	})
}
