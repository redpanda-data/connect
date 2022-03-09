package stream_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/old/input"
	"github.com/benthosdev/benthos/v4/internal/old/output"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
	"github.com/benthosdev/benthos/v4/internal/stream"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestTypeConstruction(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Input.Nanomsg.PollTimeout = "100ms"
	conf.Buffer.Type = "memory"
	conf.Output.Type = output.TypeNanomsg

	newMgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)

	assert.NoError(t, strm.Stop(time.Minute))

	newStats := metrics.Noop()
	newLogger := log.Noop()
	newMgr, err = manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), newLogger, newStats)
	require.NoError(t, err)

	strm, err = stream.New(conf, newMgr)
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

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
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

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
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

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))
}
