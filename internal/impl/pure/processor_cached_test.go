package pure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestCachedHappy(t *testing.T) {
	conf, err := newCachedProcessorConfigSpec().ParseYAML(`
key: ${! content() }
ttl: 3660s
cache: foo
skip_on: errored()
processors:
  - bloblang: 'root = content().string() + " FOO " + uuid_v4()'
  - bloblang: 'root = content().string() + " BAR " + uuid_v4()'
`, nil)
	require.NoError(t, err)

	mRes := service.MockResources(
		service.MockResourcesOptAddCache("foo"),
		service.MockResourcesOptAddCache("bar"),
	)

	proc, err := newCachedProcessorFromParsedConf(mRes, conf)
	require.NoError(t, err)

	tCtx := context.Background()

	resBatchA1, err := proc.Process(tCtx, service.NewMessage([]byte("keya")))
	require.NoError(t, err)
	require.Len(t, resBatchA1, 1)

	resBatchB1, err := proc.Process(tCtx, service.NewMessage([]byte("keyb")))
	require.NoError(t, err)
	require.Len(t, resBatchB1, 1)

	resBatchA2, err := proc.Process(tCtx, service.NewMessage([]byte("keya")))
	require.NoError(t, err)
	require.Len(t, resBatchA2, 1)

	resBatchB2, err := proc.Process(tCtx, service.NewMessage([]byte("keyb")))
	require.NoError(t, err)
	require.Len(t, resBatchB2, 1)

	resBytesA1, err := resBatchA1[0].AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(resBytesA1), "keya FOO ")
	assert.Contains(t, string(resBytesA1), " BAR ")

	resBytesB1, err := resBatchB1[0].AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(resBytesB1), "keyb FOO ")
	assert.Contains(t, string(resBytesB1), " BAR ")

	resBytesA2, err := resBatchA2[0].AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(resBytesA2), "keya FOO ")
	assert.Contains(t, string(resBytesA2), " BAR ")

	resBytesB2, err := resBatchB2[0].AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(resBytesB2), "keyb FOO ")
	assert.Contains(t, string(resBytesB2), " BAR ")

	assert.Equal(t, string(resBytesA1), string(resBytesA2))
	assert.Equal(t, string(resBytesB1), string(resBytesB2))

	require.NoError(t, mRes.AccessCache(tCtx, "foo", func(c service.Cache) {
		_, err = c.Get(tCtx, "keya")
		assert.NoError(t, err)

		_, err = c.Get(tCtx, "keyb")
		assert.NoError(t, err)
	}))

	require.NoError(t, mRes.AccessCache(tCtx, "bar", func(c service.Cache) {
		_, err = c.Get(tCtx, "keya")
		assert.Error(t, err)

		_, err = c.Get(tCtx, "keyb")
		assert.Error(t, err)
	}))

	assert.NoError(t, proc.Close(tCtx))
}

func TestCachedHappyBatched(t *testing.T) {
	conf, err := newCachedProcessorConfigSpec().ParseYAML(`
key: ${! meta("key") }
ttl: ${! meta("ttl").or("60s")}
cache: foo
processors:
  - bloblang: 'root = this.map_each(ele -> ele + " FOO")'
  - unarchive:
      format: json_array
`, nil)
	require.NoError(t, err)

	mRes := service.MockResources(service.MockResourcesOptAddCache("foo"))

	proc, err := newCachedProcessorFromParsedConf(mRes, conf)
	require.NoError(t, err)

	tCtx := context.Background()

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("key", "keya")
	msg.MetaSet("ttl", "3660s")

	resBatchA1, err := proc.Process(tCtx, service.NewMessage([]byte(`["valuea","valueb","valuec"]`)))
	require.NoError(t, err)
	require.Len(t, resBatchA1, 3)

	msg = service.NewMessage([]byte(``))
	msg.MetaSet("key", "keya")

	resBatchA2, err := proc.Process(tCtx, service.NewMessage([]byte(`["valued","valuee"]`)))
	require.NoError(t, err)
	require.Len(t, resBatchA2, 3)

	resBytes, err := resBatchA1[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `"valuea FOO"`, string(resBytes))

	resBytes, err = resBatchA1[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `"valueb FOO"`, string(resBytes))

	resBytes, err = resBatchA1[2].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `"valuec FOO"`, string(resBytes))

	resBytes, err = resBatchA2[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `"valuea FOO"`, string(resBytes))

	resBytes, err = resBatchA2[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `"valueb FOO"`, string(resBytes))

	resBytes, err = resBatchA2[2].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `"valuec FOO"`, string(resBytes))

	assert.NoError(t, proc.Close(tCtx))
}

func TestCachedSkip(t *testing.T) {
	conf, err := newCachedProcessorConfigSpec().ParseYAML(`
key: ${! content() }
cache: foo
skip_on: "errored()"
processors:
  - bloblang: |
      let body = content().string()
      root = "%s FOO %d".format($body, count($body))
  - bloblang: root = throw("simulated error")
`, nil)
	require.NoError(t, err)

	mRes := service.MockResources(
		service.MockResourcesOptAddCache("foo"),
		service.MockResourcesOptAddCache("bar"),
	)

	proc, err := newCachedProcessorFromParsedConf(mRes, conf)
	require.NoError(t, err)

	tCtx := context.Background()

	resBatchA1, err := proc.Process(tCtx, service.NewMessage([]byte("keya")))
	require.NoError(t, err)
	require.Len(t, resBatchA1, 1)

	resBatchA2, err := proc.Process(tCtx, service.NewMessage([]byte("keya")))
	require.NoError(t, err)
	require.Len(t, resBatchA2, 1)

	resBytesA1, err := resBatchA1[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "keya FOO 1", string(resBytesA1))

	resBytesA2, err := resBatchA2[0].AsBytes()
	require.NoError(t, err)
	// Since caching is skipped, the second message runs through processors
	assert.Equal(t, "keya FOO 2", string(resBytesA2))

	require.NoError(t, mRes.AccessCache(tCtx, "foo", func(c service.Cache) {
		val, err := c.Get(tCtx, "keya")
		assert.ErrorIs(t, err, service.ErrKeyNotFound)
		assert.Empty(t, val)
	}))

	assert.NoError(t, proc.Close(tCtx))
}

func TestCachedBadTTL(t *testing.T) {
	conf, err := newCachedProcessorConfigSpec().ParseYAML(`
key: ${! content() }
ttl: ${! 2+2 }
cache: foo
skip_on: "errored()"
processors:
  - bloblang: |
      let body = content().string()
      root = "%s FOO %d".format($body, count($body))
  - bloblang: root = throw("simulated error")
`, nil)
	require.NoError(t, err)

	mRes := service.MockResources(
		service.MockResourcesOptAddCache("foo"),
	)

	proc, err := newCachedProcessorFromParsedConf(mRes, conf)
	require.NoError(t, err)

	tCtx := context.Background()

	resBatchA1, err := proc.Process(tCtx, service.NewMessage([]byte("keya")))
	assert.EqualError(t, err, `failed to parse ttl expression: time: missing unit in duration "4"`)
	assert.Empty(t, resBatchA1)

	assert.NoError(t, proc.Close(tCtx))
}
