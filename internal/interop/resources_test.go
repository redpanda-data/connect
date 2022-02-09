package interop_test

import (
	"context"
	"testing"

	icache "github.com/Jeffail/benthos/v3/internal/component/cache"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	iratelimit "github.com/Jeffail/benthos/v3/internal/component/ratelimit"
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
	assert.NoError(t, interop.AccessCache(ctx, mgr, "foo", func(c icache.V1) {
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
	assert.NoError(t, interop.AccessProcessor(ctx, mgr, "buz", func(p iprocessor.V1) {
		pcalled = true
	}))
	assert.True(t, pcalled)
	assert.NoError(t, interop.AccessRateLimit(ctx, mgr, "bev", func(rl iratelimit.V1) {
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
