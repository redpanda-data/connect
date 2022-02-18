package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupe(t *testing.T) {
	doc1 := []byte("hello world")
	doc2 := []byte("hello world")
	doc3 := []byte("hello world 2")

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := NewConfig()
	conf.Type = "dedupe"
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Key = "${! content() }"

	proc, err := New(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)

	msgIn = message.QuickBatch([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 0)

	msgIn = message.QuickBatch([][]byte{doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)

	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = New(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	msgIn = message.QuickBatch([][]byte{doc1, doc2, doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)
	assert.Equal(t, 2, msgOut[0].Len())
}

func TestDedupeBadCache(t *testing.T) {
	conf := NewConfig()
	conf.Type = "dedupe"
	conf.Dedupe.Cache = "foocache"

	mgr := mock.NewManager()
	_, err := New(conf, mgr, log.Noop(), metrics.Noop())
	require.Error(t, err)
}

func TestDedupeCacheErrors(t *testing.T) {
	conf := NewConfig()
	conf.Type = "dedupe"
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Key = "${! content() }"

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err := New(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	delete(mgr.Caches, "foocache")

	msgs, err := proc.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	require.NoError(t, err)
	assert.Len(t, msgs, 0)

	conf.Dedupe.DropOnCacheErr = false
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = New(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	delete(mgr.Caches, "foocache")

	msgs, err = proc.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
}
