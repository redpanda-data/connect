package pure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"

	cuckoo "github.com/seiflotfy/cuckoofilter"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCuckooCacheStandard(t *testing.T) {
	t.Parallel()

	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
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

func TestCuckooCacheDelete(t *testing.T) {
	t.Parallel()

	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	ctx := context.Background()
	key := "foo"

	err = c.Set(ctx, key, nil, nil)
	assert.NoError(t, err)

	err = c.Delete(ctx, key)
	assert.NoError(t, err)

	_, err = c.Get(ctx, key)
	assert.EqualError(t, err, "key does not exist")
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

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
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

func TestCuckooImportDumpFile(t *testing.T) {
	t.Parallel()

	path, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	defer os.RemoveAll(path)

	capacity := 1000

	outer := cuckoo.NewFilter(uint(capacity))

	_ = outer.Insert([]byte("foo"))
	_ = outer.Insert([]byte("bar"))

	f, err := os.CreateTemp(path, "benthos-cuckoo-dump.*.dat")
	require.NoError(t, err)

	data := outer.Encode()

	_, err = f.Write(data)
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

	defConf, err := cuckooCacheConfig().ParseYAML(yamlStr, nil)
	require.NoErrorf(t, err, "unexpected error while parse:\n%s", yamlStr)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	defer c.Close(context.Background())

	_, err = c.Get(context.Background(), "foo")
	assert.NoError(t, err)

	_, err = c.Get(context.Background(), "bar")
	assert.NoError(t, err)

	_, err = c.Get(context.Background(), "baz")
	assert.True(t, errors.Is(err, service.ErrKeyNotFound))
}

func TestCuckooWriteDumpFile(t *testing.T) {
	t.Parallel()

	storagePath, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	defer os.RemoveAll(storagePath)

	lastFileImported := path.Join(storagePath, "benthos-cuckoo-dump.1691480368391.dat")
	yamlStr := fmt.Sprintf(`
---
cap: 1024
init_values:
    - foo
    - bar
storage:
    path: %q
`, lastFileImported)

	defConf, err := cuckooCacheConfig().ParseYAML(yamlStr, nil)
	require.NoErrorf(t, err, "unexpected error while parse:\n%s", yamlStr)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	err = c.Close(context.Background())
	require.NoError(t, err)

	f, err := os.Open(lastFileImported)
	require.NoError(t, err)

	defer f.Close()

	data, err := io.ReadAll(f)
	require.NoError(t, err)

	outer, err := cuckoo.Decode(data)
	require.NoError(t, err)

	assert.True(t, outer.Lookup([]byte("foo")))
	assert.True(t, outer.Lookup([]byte("bar")))
	assert.False(t, outer.Lookup([]byte("baz")))
}

func TestCuckooWriteDumpFileReadOnly(t *testing.T) {
	t.Parallel()

	storagePath, err := os.MkdirTemp(os.TempDir(), "*")
	require.NoError(t, err)

	defer os.RemoveAll(storagePath)

	lastFileImported := path.Join(storagePath, "benthos-cuckoo-dump.1691480368391.dat")
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

	defConf, err := cuckooCacheConfig().ParseYAML(yamlStr, nil)
	require.NoErrorf(t, err, "unexpected error while parse:\n%s", yamlStr)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
	require.NoError(t, err)

	err = c.Close(context.Background())
	require.NoError(t, err)

	_, err = os.Stat(lastFileImported)
	assert.True(t, errors.Is(err, os.ErrNotExist), "should not create file")
}

func BenchmarkCuckoo(b *testing.B) {
	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := i % cuckooCacheFieldCapDefaultValue
		key, value := fmt.Sprintf("key%v", j), []byte("t")

		err = c.Set(ctx, key, value, nil)
	}

	_ = err
}

func BenchmarkCuckooParallel(b *testing.B) {
	defConf, err := cuckooCacheConfig().ParseYAML(``, nil)
	require.NoError(b, err)

	logger := service.MockResources().Logger()

	c, err := cuckooMemCacheFromConfig(defConf, logger)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var err error

		for i := 0; p.Next(); i++ {
			j := i % cuckooCacheFieldCapDefaultValue
			key, value := fmt.Sprintf("key%v", j), []byte("t")

			err = c.Set(ctx, key, value, nil)
		}

		_ = err
	})
}
