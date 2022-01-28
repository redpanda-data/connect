package interop_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestMissingProbesNewIface(t *testing.T) {
	mgr, err := manager.NewV2(manager.NewResourceConfig(), nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	ctx := context.Background()

	assert.EqualError(t, interop.ProbeCache(ctx, mgr, "foo"), "cache resource 'foo' was not found")
	assert.EqualError(t, interop.ProbeInput(ctx, mgr, "bar"), "input resource 'bar' was not found")
	assert.EqualError(t, interop.ProbeOutput(ctx, mgr, "baz"), "output resource 'baz' was not found")
	assert.EqualError(t, interop.ProbeProcessor(ctx, mgr, "buz"), "processor resource 'buz' was not found")
	assert.EqualError(t, interop.ProbeRateLimit(ctx, mgr, "bev"), "rate limit resource 'bev' was not found")

	assert.EqualError(t, interop.AccessCache(ctx, mgr, "foo", nil), "unable to locate resource: foo")
	assert.EqualError(t, interop.AccessInput(ctx, mgr, "bar", nil), "unable to locate resource: bar")
	assert.EqualError(t, interop.AccessOutput(ctx, mgr, "baz", nil), "unable to locate resource: baz")
	assert.EqualError(t, interop.AccessProcessor(ctx, mgr, "buz", nil), "unable to locate resource: buz")
	assert.EqualError(t, interop.AccessRateLimit(ctx, mgr, "bev", nil), "unable to locate resource: bev")

	inConf := input.NewConfig()
	inConf.Type = input.TypeDynamic

	outConf := output.NewConfig()
	outConf.Type = output.TypeDynamic

	require.NoError(t, mgr.StoreCache(ctx, "foo", cache.NewConfig()))
	require.NoError(t, mgr.StoreInput(ctx, "bar", inConf))
	require.NoError(t, mgr.StoreOutput(ctx, "baz", outConf))
	require.NoError(t, mgr.StoreProcessor(ctx, "buz", processor.NewConfig()))
	require.NoError(t, mgr.StoreRateLimit(ctx, "bev", ratelimit.NewConfig()))

	assert.NoError(t, interop.ProbeCache(ctx, mgr, "foo"))
	assert.NoError(t, interop.ProbeInput(ctx, mgr, "bar"))
	assert.NoError(t, interop.ProbeOutput(ctx, mgr, "baz"))
	assert.NoError(t, interop.ProbeProcessor(ctx, mgr, "buz"))
	assert.NoError(t, interop.ProbeRateLimit(ctx, mgr, "bev"))

	var ccalled, icalled, ocalled, pcalled, rcalled bool
	assert.NoError(t, interop.AccessCache(ctx, mgr, "foo", func(c types.Cache) {
		ccalled = true
	}))
	assert.True(t, ccalled)
	assert.NoError(t, interop.AccessInput(ctx, mgr, "bar", func(i types.Input) {
		icalled = true
	}))
	assert.True(t, icalled)
	assert.NoError(t, interop.AccessOutput(ctx, mgr, "baz", func(ow types.OutputWriter) {
		ocalled = true
	}))
	assert.True(t, ocalled)
	assert.NoError(t, interop.AccessProcessor(ctx, mgr, "buz", func(p types.Processor) {
		pcalled = true
	}))
	assert.True(t, pcalled)
	assert.NoError(t, interop.AccessRateLimit(ctx, mgr, "bev", func(rl types.RateLimit) {
		rcalled = true
	}))
	assert.True(t, rcalled)
}

func TestMissingProbesNoSupport(t *testing.T) {
	ctx := context.Background()

	assert.EqualError(t, interop.ProbeInput(ctx, nil, "bar"), "manager does not support input resources")
	assert.EqualError(t, interop.ProbeOutput(ctx, nil, "baz"), "manager does not support output resources")
	assert.EqualError(t, interop.ProbeProcessor(ctx, nil, "buz"), "manager does not support processor resources")
}

type oldMgr struct {
	caches     map[string]types.Cache
	inputs     map[string]types.Input
	outputs    map[string]types.OutputWriter
	processors map[string]types.Processor
	ratelimits map[string]types.RateLimit
}

func (o *oldMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}

func (o *oldMgr) GetCache(name string) (types.Cache, error) {
	if c, ok := o.caches[name]; ok {
		return c, nil
	}
	return nil, types.ErrCacheNotFound
}

func (o *oldMgr) GetRateLimit(name string) (types.RateLimit, error) {
	if c, ok := o.ratelimits[name]; ok {
		return c, nil
	}
	return nil, types.ErrRateLimitNotFound
}

func (o *oldMgr) GetPlugin(name string) (interface{}, error) {
	return nil, types.ErrPluginNotFound
}

func (o *oldMgr) GetPipe(name string) (<-chan types.Transaction, error) {
	return nil, types.ErrPipeNotFound
}

func (o *oldMgr) SetPipe(name string, t <-chan types.Transaction) {}

func (o *oldMgr) UnsetPipe(name string, t <-chan types.Transaction) {}

type oldMgr2 struct {
	*oldMgr
}

func (o *oldMgr2) GetInput(name string) (types.Input, error) {
	if c, ok := o.inputs[name]; ok {
		return c, nil
	}
	return nil, types.ErrInputNotFound
}

func (o *oldMgr2) GetOutput(name string) (types.OutputWriter, error) {
	if c, ok := o.outputs[name]; ok {
		return c, nil
	}
	return nil, types.ErrOutputNotFound
}

func (o *oldMgr2) GetProcessor(name string) (types.Processor, error) {
	if c, ok := o.processors[name]; ok {
		return c, nil
	}
	return nil, types.ErrProcessorNotFound
}

func TestMissingProbesOldIface(t *testing.T) {
	mgr := &oldMgr{
		caches:     map[string]types.Cache{},
		inputs:     map[string]types.Input{},
		outputs:    map[string]types.OutputWriter{},
		processors: map[string]types.Processor{},
		ratelimits: map[string]types.RateLimit{},
	}
	mgr2 := &oldMgr2{mgr}

	ctx := context.Background()

	assert.EqualError(t, interop.ProbeCache(ctx, mgr, "foo"), "cache resource 'foo' was not found")
	assert.EqualError(t, interop.ProbeInput(ctx, mgr, "bar"), "manager does not support input resources")
	assert.EqualError(t, interop.ProbeInput(ctx, mgr2, "bar"), "input resource 'bar' was not found")
	assert.EqualError(t, interop.ProbeOutput(ctx, mgr, "baz"), "manager does not support output resources")
	assert.EqualError(t, interop.ProbeOutput(ctx, mgr2, "baz"), "output resource 'baz' was not found")
	assert.EqualError(t, interop.ProbeProcessor(ctx, mgr, "buz"), "manager does not support processor resources")
	assert.EqualError(t, interop.ProbeProcessor(ctx, mgr2, "buz"), "processor resource 'buz' was not found")
	assert.EqualError(t, interop.ProbeRateLimit(ctx, mgr, "bev"), "rate limit resource 'bev' was not found")

	mgr.caches["foo"] = nil
	mgr.inputs["bar"] = nil
	mgr.outputs["baz"] = nil
	mgr.processors["buz"] = nil
	mgr.ratelimits["bev"] = nil

	assert.NoError(t, interop.ProbeCache(ctx, mgr, "foo"))
	assert.EqualError(t, interop.ProbeInput(ctx, mgr, "bar"), "manager does not support input resources")
	assert.NoError(t, interop.ProbeInput(ctx, mgr2, "bar"))
	assert.EqualError(t, interop.ProbeOutput(ctx, mgr, "baz"), "manager does not support output resources")
	assert.NoError(t, interop.ProbeOutput(ctx, mgr2, "baz"))
	assert.EqualError(t, interop.ProbeProcessor(ctx, mgr, "buz"), "manager does not support processor resources")
	assert.NoError(t, interop.ProbeProcessor(ctx, mgr2, "buz"))
	assert.NoError(t, interop.ProbeRateLimit(ctx, mgr, "bev"))

	assert.EqualError(t, interop.AccessCache(ctx, mgr, "foo", nil), "cache not found")
	assert.EqualError(t, interop.AccessInput(ctx, mgr, "bar", nil), "manager does not support input resources")
	assert.EqualError(t, interop.AccessInput(ctx, mgr2, "bar", nil), "input not found")
	assert.EqualError(t, interop.AccessOutput(ctx, mgr, "baz", nil), "manager does not support output resources")
	assert.EqualError(t, interop.AccessOutput(ctx, mgr2, "baz", nil), "output not found")
	assert.EqualError(t, interop.AccessProcessor(ctx, mgr, "buz", nil), "manager does not support processor resources")
	assert.EqualError(t, interop.AccessProcessor(ctx, mgr2, "buz", nil), "processor not found")
	assert.EqualError(t, interop.AccessRateLimit(ctx, mgr, "bev", nil), "rate limit not found")
}
