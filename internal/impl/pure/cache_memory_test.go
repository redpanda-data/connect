package pure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestMemoryCache(t *testing.T) {
	defConf, err := memCacheConfig().ParseYAML(``, nil)
	require.NoError(t, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(t, err)

	ctx := context.Background()

	expErr := service.ErrKeyNotFound
	if _, act := c.Get(ctx, "foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set(ctx, "foo", []byte("1"), nil); err != nil {
		t.Error(err)
	}

	exp := "1"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err = c.Add(ctx, "bar", []byte("2"), nil); err != nil {
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

	if err = c.Set(ctx, "foo", []byte("3"), nil); err != nil {
		t.Error(err)
	}

	exp = "3"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err = c.Delete(ctx, "foo"); err != nil {
		t.Error(err)
	}

	if _, err = c.Get(ctx, "foo"); err != service.ErrKeyNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, service.ErrKeyNotFound)
	}
}

func TestMemoryCacheCompaction(t *testing.T) {
	defConf, err := memCacheConfig().ParseYAML(`
default_ttl: 0s
compaction_interval: 1ns
`, nil)
	require.NoError(t, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(t, err)

	ctx := context.Background()

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	err = c.Set(ctx, "foo", []byte("1"), nil)
	require.NoError(t, err)

	_, err = c.Get(ctx, "foo")
	assert.Equal(t, service.ErrKeyNotFound, err)

	<-time.After(time.Millisecond * 50)

	// This should trigger compaction.
	err = c.Add(ctx, "bar", []byte("2"), nil)
	require.NoError(t, err)

	_, err = c.Get(ctx, "bar")
	assert.Equal(t, service.ErrKeyNotFound, err)
}

func TestMemoryCacheInitValues(t *testing.T) {
	defConf, err := memCacheConfig().ParseYAML(`
default_ttl: 0s
compaction_interval: ""
init_values:
  foo: bar
  foo2: bar2
`, nil)
	require.NoError(t, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(t, err)

	ctx := context.Background()

	exp := "bar"
	if act, err := c.Get(ctx, "foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	// This should trigger compaction.
	if err = c.Add(ctx, "foo3", []byte("bar3"), nil); err != nil {
		t.Error(err)
	}

	exp = "bar"
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

func TestMemoryCacheCompactionOnRead(t *testing.T) {
	defConf, err := memCacheConfig().ParseYAML(`
default_ttl: 0s
compaction_interval: 1ns
`, nil)
	require.NoError(t, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(t, err)

	ctx := context.Background()

	expErr := service.ErrKeyNotFound
	if _, act := c.Get(ctx, "foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set(ctx, "foo", []byte("1"), nil); err != nil {
		t.Error(err)
	}

	<-time.After(time.Millisecond * 50)

	// This should trigger compaction.
	if _, act := c.Get(ctx, "foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}
}

//------------------------------------------------------------------------------

func BenchmarkMemoryShards1(b *testing.B) {
	defConf, err := memCacheConfig().ParseYAML(`
default_ttl: 0s
compaction_interval: ""
`, nil)
	require.NoError(b, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%v", i), []byte(fmt.Sprintf("foo%v", i))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkMemoryShards10(b *testing.B) {
	defConf, err := memCacheConfig().ParseYAML(`
default_ttl: 0s
compaction_interval: ""
shards: 10
`, nil)
	require.NoError(b, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%v", i), []byte(fmt.Sprintf("foo%v", i))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkMemoryShards100(b *testing.B) {
	defConf, err := memCacheConfig().ParseYAML(`
default_ttl: 0s
compaction_interval: ""
shards: 10
`, nil)
	require.NoError(b, err)

	c, err := newMemCacheFromConfig(defConf)
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%v", i), []byte(fmt.Sprintf("foo%v", i))

		assert.NoError(b, c.Set(ctx, key, value, nil))

		res, err := c.Get(ctx, key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}
