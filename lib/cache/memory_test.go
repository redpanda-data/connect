package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

func TestMemoryCache(t *testing.T) {
	testLog := log.Noop()

	conf := NewConfig()
	conf.Type = "memory"

	c, err := New(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	expErr := types.ErrKeyNotFound
	if _, act := c.Get("foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set("foo", []byte("1")); err != nil {
		t.Error(err)
	}

	exp := "1"
	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err = c.Add("bar", []byte("2")); err != nil {
		t.Error(err)
	}

	exp = "2"
	if act, err := c.Get("bar"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	expErr = types.ErrKeyAlreadyExists
	if act := c.Add("foo", []byte("2")); expErr != act {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set("foo", []byte("3")); err != nil {
		t.Error(err)
	}

	exp = "3"
	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err = c.Delete("foo"); err != nil {
		t.Error(err)
	}

	if _, err = c.Get("foo"); err != types.ErrKeyNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrKeyNotFound)
	}
}

func TestMemoryCacheCompaction(t *testing.T) {
	conf := NewConfig()
	conf.Type = "memory"
	conf.Memory.TTL = 0
	conf.Memory.CompactionInterval = "1ns"

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	m, ok := c.(*Memory)
	require.True(t, ok)

	_, err = c.Get("foo")
	assert.Equal(t, types.ErrKeyNotFound, err)

	err = c.Set("foo", []byte("1"))
	require.NoError(t, err)

	_, exists := m.getShard("foo").items["foo"]
	assert.True(t, exists, "Item should not yet be removed during compaction")

	_, err = c.Get("foo")
	assert.Equal(t, types.ErrKeyNotFound, err)

	<-time.After(time.Millisecond * 50)

	// This should trigger compaction.
	err = c.Add("bar", []byte("2"))
	require.NoError(t, err)

	_, exists = m.getShard("bar").items["bar"]
	assert.True(t, exists)

	_, err = c.Get("bar")
	assert.Equal(t, types.ErrKeyNotFound, err)

	_, exists = m.getShard("foo").items["foo"]
	assert.False(t, exists, "Item should be removed during compaction")
}

func TestMemoryCacheInitValues(t *testing.T) {
	conf := NewConfig()
	conf.Type = "memory"
	conf.Memory.TTL = 0
	conf.Memory.CompactionInterval = ""
	conf.Memory.InitValues = map[string]string{
		"foo":  "bar",
		"foo2": "bar2",
	}

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := "bar"
	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	// This should trigger compaction.
	if err = c.Add("foo3", []byte("bar3")); err != nil {
		t.Error(err)
	}

	exp = "bar"
	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	exp = "bar2"
	if act, err := c.Get("foo2"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}
}

func TestMemoryCacheCompactionOnRead(t *testing.T) {
	testLog := log.Noop()

	conf := NewConfig()
	conf.Type = "memory"
	conf.Memory.TTL = 0
	conf.Memory.CompactionInterval = "1ns"

	c, err := New(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	expErr := types.ErrKeyNotFound
	if _, act := c.Get("foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set("foo", []byte("1")); err != nil {
		t.Error(err)
	}

	<-time.After(time.Millisecond * 50)

	// This should trigger compaction.
	if _, act := c.Get("foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}
}

//------------------------------------------------------------------------------

func BenchmarkMemoryShards1(b *testing.B) {
	conf := NewConfig()
	conf.Type = TypeMemory
	conf.Memory.TTL = 0
	conf.Memory.CompactionInterval = ""

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%v", i), []byte(fmt.Sprintf("foo%v", i))

		assert.NoError(b, c.Set(key, value))

		res, err := c.Get(key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkMemoryShards10(b *testing.B) {
	conf := NewConfig()
	conf.Type = TypeMemory
	conf.Memory.TTL = 0
	conf.Memory.CompactionInterval = ""
	conf.Memory.Shards = 10

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%v", i), []byte(fmt.Sprintf("foo%v", i))

		assert.NoError(b, c.Set(key, value))

		res, err := c.Get(key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}

func BenchmarkMemoryShards100(b *testing.B) {
	conf := NewConfig()
	conf.Type = TypeMemory
	conf.Memory.TTL = 0
	conf.Memory.CompactionInterval = ""
	conf.Memory.Shards = 10

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%v", i), []byte(fmt.Sprintf("foo%v", i))

		assert.NoError(b, c.Set(key, value))

		res, err := c.Get(key)
		require.NoError(b, err)
		assert.Equal(b, value, res)
	}
}
