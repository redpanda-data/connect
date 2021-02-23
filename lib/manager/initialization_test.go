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
	mgr, err := New(NewConfig(), nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr.bufferBundle = &bundle.BufferSet{}
	mgr.cacheBundle = &bundle.CacheSet{}
	mgr.inputBundle = &bundle.InputSet{}
	mgr.outputBundle = &bundle.OutputSet{}
	mgr.processorBundle = &bundle.ProcessorSet{}
	mgr.rateLimitBundle = &bundle.RateLimitSet{}

	mgr.bufferBundle.Add(func(c buffer.Config, mgr bundle.NewManagement) (buffer.Type, error) {
		return nil, errors.New("not this buffer")
	}, docs.ComponentSpec{
		Name: "testbuffer",
	})

	mgr.cacheBundle.Add(func(c cache.Config, mgr bundle.NewManagement) (types.Cache, error) {
		return nil, errors.New("not this cache")
	}, docs.ComponentSpec{
		Name: "testcache",
	})

	lenInputProcs := 0
	mgr.inputBundle.Add(func(b bool, c input.Config, mgr bundle.NewManagement, p ...types.PipelineConstructorFunc) (input.Type, error) {
		lenInputProcs = len(p)
		return nil, errors.New("not this input")
	}, docs.ComponentSpec{
		Name: "testinput",
	})

	lenOutputProcs := 0
	mgr.outputBundle.Add(func(c output.Config, mgr bundle.NewManagement, p ...types.PipelineConstructorFunc) (output.Type, error) {
		lenOutputProcs = len(p)
		return nil, errors.New("not this output")
	}, docs.ComponentSpec{
		Name: "testoutput",
	})

	mgr.processorBundle.Add(func(c processor.Config, mgr bundle.NewManagement) (processor.Type, error) {
		return nil, errors.New("not this processor")
	}, docs.ComponentSpec{
		Name: "testprocessor",
	})

	mgr.rateLimitBundle.Add(func(c ratelimit.Config, mgr bundle.NewManagement) (types.RateLimit, error) {
		return nil, errors.New("not this rate limit")
	}, docs.ComponentSpec{
		Name: "testratelimit",
	})

	bConf := buffer.NewConfig()
	bConf.Type = "testbuffer"
	_, err = mgr.NewBuffer(bConf)
	assert.EqualError(t, err, "not this buffer")

	cConf := cache.NewConfig()
	cConf.Type = "testcache"
	_, err = mgr.NewCache(cConf)
	assert.EqualError(t, err, "not this cache")

	iConf := input.NewConfig()
	iConf.Type = "testinput"
	_, err = mgr.NewInput(iConf, true, nil, nil)
	assert.EqualError(t, err, "not this input")
	assert.Equal(t, 2, lenInputProcs)

	oConf := output.NewConfig()
	oConf.Type = "testoutput"
	_, err = mgr.NewOutput(oConf, nil, nil, nil)
	assert.EqualError(t, err, "not this output")
	assert.Equal(t, 3, lenOutputProcs)

	pConf := processor.NewConfig()
	pConf.Type = "testprocessor"
	_, err = mgr.NewProcessor(pConf)
	assert.EqualError(t, err, "not this processor")

	rConf := ratelimit.NewConfig()
	rConf.Type = "testratelimit"
	_, err = mgr.NewRateLimit(rConf)
	assert.EqualError(t, err, "not this rate limit")
}
