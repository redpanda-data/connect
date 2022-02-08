package stream_test

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestTypeConstruction(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Input.Nanomsg.PollTimeout = "100ms"
	conf.Buffer.Type = "memory"
	conf.Output.Type = output.TypeNanomsg

	newMgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	strm, err := stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)

	assert.NoError(t, strm.Stop(time.Minute))

	newStats := metrics.Noop()
	newLogger := log.Noop()
	newMgr, err = manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), newLogger, newStats)
	require.NoError(t, err)

	strm, err = stream.New(conf, stream.OptSetLogger(newLogger), stream.OptSetStats(newStats), stream.OptSetManager(newMgr))
	require.NoError(t, err)

	require.NoError(t, strm.Stop(time.Minute))
}

func TestTypeCloseGracefully(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = input.TypeHTTPServer
	conf.Buffer.Type = "memory"
	conf.Output.Type = output.TypeHTTPServer

	newMgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	strm, err := stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))

	strm, err = stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))
}

func TestTypeCloseOrdered(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = input.TypeHTTPServer
	conf.Buffer.Type = "memory"
	conf.Output.Type = output.TypeHTTPServer

	newMgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	strm, err := stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))

	strm, err = stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))
}

func TestTypeCloseUnordered(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = input.TypeHTTPServer
	conf.Buffer.Type = "memory"
	conf.Output.Type = output.TypeHTTPServer

	newMgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	strm, err := stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))

	strm, err = stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, stream.OptSetManager(newMgr))
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))
}
