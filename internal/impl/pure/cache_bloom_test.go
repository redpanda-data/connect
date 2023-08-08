package pure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	bloom "github.com/bits-and-blooms/bloom/v3"
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

func TestBloomImportDumpFile(t *testing.T) {
	t.Parallel()

	path, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	defer os.RemoveAll(path)

	capacity := 1000
	fp := 0.01

	outer := bloom.NewWithEstimates(uint(capacity), fp)

	outer.AddString("foo").AddString("bar")

	f, err := os.CreateTemp(path, "benthos-bloom-dump.*.dat")
	require.NoError(t, err)

	_, err = outer.WriteTo(f)
	require.NoError(t, err)

	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	yamlStr := fmt.Sprintf(`
---
cap: 1024
init_values:
    - foo
    - bar
storage:
    path: %q
`, path)

	defConf, err := bloomCacheConfig().ParseYAML(yamlStr, nil)
	require.NoErrorf(t, err, "unexpected error while parse:\n%s", yamlStr)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	defer c.Close(context.Background())

	_, err = c.Get(context.Background(), "foo")
	assert.NoError(t, err)

	_, err = c.Get(context.Background(), "bar")
	assert.NoError(t, err)

	_, err = c.Get(context.Background(), "baz")
	assert.True(t, errors.Is(err, service.ErrKeyNotFound))
}

func TestBloomWriteDumpFile(t *testing.T) {
	t.Parallel()

	storagePath, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	defer os.RemoveAll(storagePath)

	lastFileImported := path.Join(storagePath, "benthos-bloom-dump.1691480368391.dat")
	yamlStr := fmt.Sprintf(`
---
cap: 1024
init_values:
    - foo
    - bar
storage:
    path: %q
`, lastFileImported)

	defConf, err := bloomCacheConfig().ParseYAML(yamlStr, nil)
	require.NoErrorf(t, err, "unexpected error while parse:\n%s", yamlStr)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	err = c.Close(context.Background())
	require.NoError(t, err)

	f, err := os.Open(lastFileImported)
	require.NoError(t, err)

	defer f.Close()

	capacity := 1000
	fp := 0.01

	outer := bloom.NewWithEstimates(uint(capacity), fp)

	_, err = outer.ReadFrom(f)
	require.NoError(t, err)

	assert.True(t, outer.TestString("foo"))
	assert.True(t, outer.TestString("bar"))
	assert.False(t, outer.TestString("baz"))
}

func TestBloomWriteDumpFileReadOnly(t *testing.T) {
	t.Parallel()

	storagePath, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	defer os.RemoveAll(storagePath)

	lastFileImported := path.Join(storagePath, "benthos-bloom-dump.1691480368391.dat")
	yamlStr := fmt.Sprintf(`
---
cap: 1024
init_values:
    - foo
    - bar
storage:
    path: %q
    read_only: true
`, lastFileImported)

	defConf, err := bloomCacheConfig().ParseYAML(yamlStr, nil)
	require.NoErrorf(t, err, "unexpected error while parse:\n%s", yamlStr)

	logger := service.MockResources().Logger()

	c, err := bloomMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	err = c.Close(context.Background())
	require.NoError(t, err)

	_, err = os.Stat(lastFileImported)
	assert.True(t, errors.Is(err, os.ErrNotExist), "should not create file")
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
