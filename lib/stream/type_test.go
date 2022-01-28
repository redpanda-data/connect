package stream

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeConstruction(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Input.Nanomsg.PollTimeout = "100ms"
	conf.Output.Type = output.TypeNanomsg

	strm, err := New(conf) // nanomsg => nanomsg
	require.NoError(t, err)

	assert.NotNil(t, strm.logger)
	assert.NotNil(t, strm.stats)
	assert.NotNil(t, strm.manager)

	assert.NoError(t, strm.Stop(time.Minute))

	newStats := metrics.Noop()
	newLogger := log.Noop()
	newMgr := types.NoopMgr()

	strm, err = New(conf, OptSetLogger(newLogger), OptSetStats(newStats), OptSetManager(newMgr))
	require.NoError(t, err)

	assert.Equal(t, newLogger, strm.logger)
	assert.Equal(t, newStats, strm.stats)
	assert.Equal(t, newMgr, strm.manager)
}

func TestTypeCloseGracefully(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeHTTPServer
	conf.Output.Type = output.TypeHTTPServer

	strm, err := New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopGracefully(time.Minute))

	conf.Buffer.Type = "memory"

	strm, err = New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopGracefully(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopGracefully(time.Minute))
}

func TestTypeCloseOrdered(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Output.Type = output.TypeNanomsg

	strm, err := New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopOrdered(time.Minute))

	conf.Buffer.Type = "memory"

	strm, err = New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopOrdered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopOrdered(time.Minute))
}

func TestTypeCloseUnordered(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Output.Type = input.TypeNanomsg

	strm, err := New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopUnordered(time.Minute))

	conf.Buffer.Type = "memory"

	strm, err = New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopUnordered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = New(conf)
	require.NoError(t, err)
	assert.NoError(t, strm.stopUnordered(time.Minute))
}
