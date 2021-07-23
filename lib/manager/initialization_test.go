package manager

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitialization(t *testing.T) {
	env := bundle.NewEnvironment()

	env.Buffers.Add(func(c buffer.Config, mgr bundle.NewManagement) (buffer.Type, error) {
		return nil, errors.New("not this buffer")
	}, docs.ComponentSpec{
		Name: "testbuffer",
	})

	env.Caches.Add(func(c cache.Config, mgr bundle.NewManagement) (types.Cache, error) {
		return nil, errors.New("not this cache")
	}, docs.ComponentSpec{
		Name: "testcache",
	})

	lenInputProcs := 0
	env.Inputs.Add(func(b bool, c input.Config, mgr bundle.NewManagement, p ...types.PipelineConstructorFunc) (input.Type, error) {
		lenInputProcs = len(p)
		return nil, errors.New("not this input")
	}, docs.ComponentSpec{
		Name: "testinput",
	})

	lenOutputProcs := 0
	env.Outputs.Add(func(c output.Config, mgr bundle.NewManagement, p ...types.PipelineConstructorFunc) (output.Type, error) {
		lenOutputProcs = len(p)
		return nil, errors.New("not this output")
	}, docs.ComponentSpec{
		Name: "testoutput",
	})

	env.Processors.Add(func(c processor.Config, mgr bundle.NewManagement) (processor.Type, error) {
		return nil, errors.New("not this processor")
	}, docs.ComponentSpec{
		Name: "testprocessor",
	})

	env.RateLimits.Add(func(c ratelimit.Config, mgr bundle.NewManagement) (types.RateLimit, error) {
		return nil, errors.New("not this rate limit")
	}, docs.ComponentSpec{
		Name: "testratelimit",
	})

	mgr, err := NewV2(NewResourceConfig(), nil, log.Noop(), metrics.Noop(), OptSetEnvironment(env))
	require.NoError(t, err)

	bConf := buffer.NewConfig()
	bConf.Type = "testbuffer"
	_, err = mgr.NewBuffer(bConf)
	assert.EqualError(t, err, "not this buffer")

	_, err = buffer.New(bConf, mgr, log.Noop(), metrics.Noop())
	assert.EqualError(t, err, "not this buffer")

	cConf := cache.NewConfig()
	cConf.Type = "testcache"
	_, err = mgr.NewCache(cConf)
	assert.EqualError(t, err, "not this cache")

	_, err = cache.New(cConf, mgr, log.Noop(), metrics.Noop())
	assert.EqualError(t, err, "not this cache")

	iConf := input.NewConfig()
	iConf.Type = "testinput"
	_, err = mgr.NewInput(iConf, true, nil, nil)
	assert.EqualError(t, err, "not this input")
	assert.Equal(t, 2, lenInputProcs)

	_, err = input.New(iConf, mgr, log.Noop(), metrics.Noop())
	assert.EqualError(t, err, "not this input")

	oConf := output.NewConfig()
	oConf.Type = "testoutput"
	_, err = mgr.NewOutput(oConf, nil, nil, nil)
	assert.EqualError(t, err, "not this output")
	assert.Equal(t, 3, lenOutputProcs)

	_, err = output.New(oConf, mgr, log.Noop(), metrics.Noop())
	assert.EqualError(t, err, "not this output")

	pConf := processor.NewConfig()
	pConf.Type = "testprocessor"
	_, err = mgr.NewProcessor(pConf)
	assert.EqualError(t, err, "not this processor")

	_, err = processor.New(pConf, mgr, log.Noop(), metrics.Noop())
	assert.EqualError(t, err, "not this processor")

	rConf := ratelimit.NewConfig()
	rConf.Type = "testratelimit"
	_, err = mgr.NewRateLimit(rConf)
	assert.EqualError(t, err, "not this rate limit")

	_, err = ratelimit.New(rConf, mgr, log.Noop(), metrics.Noop())
	assert.EqualError(t, err, "not this rate limit")
}
