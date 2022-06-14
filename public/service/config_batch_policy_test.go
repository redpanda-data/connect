package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/manager"
)

func TestConfigBatching(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBatchPolicyField("a"))

	parsedConfig, err := spec.ParseYAML(`
a:
  count: 20
  period: 5s
  processors:
    - bloblang: 'root = content().uppercase()'
`, nil)
	require.NoError(t, err)

	_, err = parsedConfig.FieldTLS("b")
	require.Error(t, err)

	bConf, err := parsedConfig.FieldBatchPolicy("a")
	require.NoError(t, err)

	assert.Equal(t, 20, bConf.Count)
	assert.Equal(t, "5s", bConf.Period)
	require.Len(t, bConf.procs, 1)
	assert.Equal(t, "bloblang", bConf.procs[0].Type)
	assert.Equal(t, "root = content().uppercase()", bConf.procs[0].Bloblang)
}

func TestBatcherPeriod(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBatchPolicyField("a"))

	parsedConfig, err := spec.ParseYAML(`
a:
  period: 300ms
  processors:
    - bloblang: 'root = content().uppercase()'
`, nil)
	require.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	res := newResourcesFromManager(mgr)

	bConf, err := parsedConfig.FieldBatchPolicy("a")
	require.NoError(t, err)

	pol, err := bConf.NewBatcher(res)
	require.NoError(t, err)

	assert.False(t, pol.Add(NewMessage([]byte("foo"))))
	assert.False(t, pol.Add(NewMessage([]byte("bar"))))

	v, ok := pol.UntilNext()
	assert.True(t, ok)
	assert.InDelta(t, int(time.Millisecond*300), int(v), float64(time.Millisecond*50))

	batch, err := pol.Flush(context.Background())
	require.NoError(t, err)
	require.Len(t, batch, 2)

	bOne, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "FOO", string(bOne))

	bTwo, err := batch[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "BAR", string(bTwo))

	batch, err = pol.Flush(context.Background())
	require.NoError(t, err)
	require.Len(t, batch, 0)

	require.NoError(t, pol.Close(context.Background()))
}

func TestBatcherSize(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBatchPolicyField("a"))

	parsedConfig, err := spec.ParseYAML(`
a:
  count: 3
`, nil)
	require.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	res := newResourcesFromManager(mgr)

	bConf, err := parsedConfig.FieldBatchPolicy("a")
	require.NoError(t, err)

	pol, err := bConf.NewBatcher(res)
	require.NoError(t, err)

	_, ok := pol.UntilNext()
	assert.False(t, ok)

	assert.False(t, pol.Add(NewMessage([]byte("foo"))))
	assert.False(t, pol.Add(NewMessage([]byte("bar"))))
	assert.True(t, pol.Add(NewMessage([]byte("baz"))))

	_, ok = pol.UntilNext()
	assert.False(t, ok)

	batch, err := pol.Flush(context.Background())
	require.NoError(t, err)
	require.Len(t, batch, 3)

	bRes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "foo", string(bRes))

	bRes, err = batch[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "bar", string(bRes))

	bRes, err = batch[2].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "baz", string(bRes))

	batch, err = pol.Flush(context.Background())
	require.NoError(t, err)
	require.Len(t, batch, 0)

	require.NoError(t, pol.Close(context.Background()))
}
