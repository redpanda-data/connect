package manager_test

import (
	"context"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/component"
	icache "github.com/Jeffail/benthos/v3/internal/component/cache"
	iinput "github.com/Jeffail/benthos/v3/internal/component/input"
	ioutput "github.com/Jeffail/benthos/v3/internal/component/output"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	iratelimit "github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

//------------------------------------------------------------------------------

func TestManagerProcessorLabels(t *testing.T) {
	t.Skip("No longer validating labels at construction")

	goodLabels := []string{
		"foo",
		"foo_bar",
		"foo_bar_baz_buz",
		"foo__",
		"foo123__45",
	}
	for _, l := range goodLabels {
		conf := processor.NewConfig()
		conf.Type = processor.TypeBloblang
		conf.Bloblang = "root = this"
		conf.Label = l

		mgr, err := manager.NewV2(manager.NewResourceConfig(), nil, log.Noop(), metrics.Noop())
		require.NoError(t, err)

		_, err = mgr.NewProcessor(conf)
		assert.NoError(t, err, "label: %v", l)
	}

	badLabels := []string{
		"_foo",
		"foo-bar",
		"FOO",
		"foo.bar",
	}
	for _, l := range badLabels {
		conf := processor.NewConfig()
		conf.Type = processor.TypeBloblang
		conf.Bloblang = "root = this"
		conf.Label = l

		mgr, err := manager.NewV2(manager.NewResourceConfig(), nil, log.Noop(), metrics.Noop())
		require.NoError(t, err)

		_, err = mgr.NewProcessor(conf)
		assert.EqualError(t, err, docs.ErrBadLabel.Error(), "label: %v", l)
	}
}

func TestManagerCache(t *testing.T) {
	testLog := log.Noop()

	conf := manager.NewResourceConfig()

	fooCache := cache.NewConfig()
	fooCache.Label = "foo"
	conf.ResourceCaches = append(conf.ResourceCaches, fooCache)

	barCache := cache.NewConfig()
	barCache.Label = "bar"
	conf.ResourceCaches = append(conf.ResourceCaches, barCache)

	mgr, err := manager.NewV2(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetCache("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetCache("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetCache("baz"); err != component.ErrCacheNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, component.ErrCacheNotFound)
	}
}

func TestManagerCacheList(t *testing.T) {
	cacheFoo := cache.NewConfig()
	cacheFoo.Label = "foo"

	cacheBar := cache.NewConfig()
	cacheBar.Label = "bar"

	conf := manager.NewResourceConfig()
	conf.ResourceCaches = append(conf.ResourceCaches, cacheFoo, cacheBar)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = mgr.AccessCache(context.Background(), "foo", func(icache.V1) {})
	require.NoError(t, err)

	err = mgr.AccessCache(context.Background(), "bar", func(icache.V1) {})
	require.NoError(t, err)

	err = mgr.AccessCache(context.Background(), "baz", func(icache.V1) {})
	assert.EqualError(t, err, "unable to locate resource: baz")
}

func TestManagerCacheListErrors(t *testing.T) {
	cFoo := cache.NewConfig()
	cFoo.Label = "foo"

	cBar := cache.NewConfig()
	cBar.Label = "foo"

	conf := manager.NewResourceConfig()
	conf.ResourceCaches = append(conf.ResourceCaches, cFoo, cBar)

	_, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "cache resource label 'foo' collides with a previously defined resource")

	cEmpty := cache.NewConfig()
	conf = manager.NewResourceConfig()
	conf.ResourceCaches = append(conf.ResourceCaches, cEmpty)

	_, err = manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "cache resource has an empty label")
}

func TestManagerBadCache(t *testing.T) {
	testLog := log.Noop()

	conf := manager.NewResourceConfig()

	badConf := cache.NewConfig()
	badConf.Label = "bad"
	badConf.Type = "notexist"
	conf.ResourceCaches = append(conf.ResourceCaches, badConf)

	if _, err := manager.NewV2(conf, nil, testLog, metrics.Noop()); err == nil {
		t.Fatal("Expected error from bad cache")
	}
}

func TestManagerRateLimit(t *testing.T) {
	conf := manager.NewResourceConfig()

	fooRL := ratelimit.NewConfig()
	fooRL.Label = "foo"
	conf.ResourceRateLimits = append(conf.ResourceRateLimits, fooRL)

	barRL := ratelimit.NewConfig()
	barRL.Label = "bar"
	conf.ResourceRateLimits = append(conf.ResourceRateLimits, barRL)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetRateLimit("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetRateLimit("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetRateLimit("baz"); err != component.ErrRateLimitNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, component.ErrRateLimitNotFound)
	}
}

func TestManagerRateLimitList(t *testing.T) {
	cFoo := ratelimit.NewConfig()
	cFoo.Label = "foo"

	cBar := ratelimit.NewConfig()
	cBar.Label = "bar"

	conf := manager.NewResourceConfig()
	conf.ResourceRateLimits = append(conf.ResourceRateLimits, cFoo, cBar)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = mgr.AccessRateLimit(context.Background(), "foo", func(iratelimit.V1) {})
	require.NoError(t, err)

	err = mgr.AccessRateLimit(context.Background(), "bar", func(iratelimit.V1) {})
	require.NoError(t, err)

	err = mgr.AccessRateLimit(context.Background(), "baz", func(iratelimit.V1) {})
	assert.EqualError(t, err, "unable to locate resource: baz")
}

func TestManagerRateLimitListErrors(t *testing.T) {
	cFoo := ratelimit.NewConfig()
	cFoo.Label = "foo"

	cBar := ratelimit.NewConfig()
	cBar.Label = "foo"

	conf := manager.NewResourceConfig()
	conf.ResourceRateLimits = append(conf.ResourceRateLimits, cFoo, cBar)

	_, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "rate limit resource label 'foo' collides with a previously defined resource")

	cEmpty := ratelimit.NewConfig()
	conf = manager.NewResourceConfig()
	conf.ResourceRateLimits = append(conf.ResourceRateLimits, cEmpty)

	_, err = manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "rate limit resource has an empty label")
}

func TestManagerBadRateLimit(t *testing.T) {
	conf := manager.NewResourceConfig()
	badConf := ratelimit.NewConfig()
	badConf.Type = "notexist"
	badConf.Label = "bad"
	conf.ResourceRateLimits = append(conf.ResourceRateLimits, badConf)

	if _, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Fatal("Expected error from bad rate limit")
	}
}

func TestManagerProcessor(t *testing.T) {
	conf := manager.NewResourceConfig()

	fooProc := processor.NewConfig()
	fooProc.Label = "foo"
	conf.ResourceProcessors = append(conf.ResourceProcessors, fooProc)

	barProc := processor.NewConfig()
	barProc.Label = "bar"
	conf.ResourceProcessors = append(conf.ResourceProcessors, barProc)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetProcessor("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetProcessor("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetProcessor("baz"); err != component.ErrProcessorNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, component.ErrProcessorNotFound)
	}
}

func TestManagerProcessorList(t *testing.T) {
	cFoo := processor.NewConfig()
	cFoo.Label = "foo"

	cBar := processor.NewConfig()
	cBar.Label = "bar"

	conf := manager.NewResourceConfig()
	conf.ResourceProcessors = append(conf.ResourceProcessors, cFoo, cBar)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = mgr.AccessProcessor(context.Background(), "foo", func(iprocessor.V1) {})
	require.NoError(t, err)

	err = mgr.AccessProcessor(context.Background(), "bar", func(iprocessor.V1) {})
	require.NoError(t, err)

	err = mgr.AccessProcessor(context.Background(), "baz", func(iprocessor.V1) {})
	assert.EqualError(t, err, "unable to locate resource: baz")
}

func TestManagerProcessorListErrors(t *testing.T) {
	cFoo := processor.NewConfig()
	cFoo.Label = "foo"

	cBar := processor.NewConfig()
	cBar.Label = "foo"

	conf := manager.NewResourceConfig()
	conf.ResourceProcessors = append(conf.ResourceProcessors, cFoo, cBar)

	_, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "processor resource label 'foo' collides with a previously defined resource")

	cEmpty := processor.NewConfig()
	conf = manager.NewResourceConfig()
	conf.ResourceProcessors = append(conf.ResourceProcessors, cEmpty)

	_, err = manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "processor resource has an empty label")
}

func TestManagerInputList(t *testing.T) {
	cFoo := input.NewConfig()
	cFoo.Type = input.TypeHTTPServer
	cFoo.Label = "foo"

	cBar := input.NewConfig()
	cBar.Type = input.TypeHTTPServer
	cBar.Label = "bar"

	conf := manager.NewResourceConfig()
	conf.ResourceInputs = append(conf.ResourceInputs, cFoo, cBar)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = mgr.AccessInput(context.Background(), "foo", func(i iinput.Streamed) {})
	require.NoError(t, err)

	err = mgr.AccessInput(context.Background(), "bar", func(i iinput.Streamed) {})
	require.NoError(t, err)

	err = mgr.AccessInput(context.Background(), "baz", func(i iinput.Streamed) {})
	assert.EqualError(t, err, "unable to locate resource: baz")
}

func TestManagerInputListErrors(t *testing.T) {
	cFoo := input.NewConfig()
	cFoo.Label = "foo"

	cBar := input.NewConfig()
	cBar.Label = "foo"

	conf := manager.NewResourceConfig()
	conf.ResourceInputs = append(conf.ResourceInputs, cFoo, cBar)

	_, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "input resource label 'foo' collides with a previously defined resource")

	cEmpty := input.NewConfig()
	conf = manager.NewResourceConfig()
	conf.ResourceInputs = append(conf.ResourceInputs, cEmpty)

	_, err = manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "input resource has an empty label")
}

func TestManagerOutputList(t *testing.T) {
	cFoo := output.NewConfig()
	cFoo.Type = output.TypeHTTPServer
	cFoo.Label = "foo"

	cBar := output.NewConfig()
	cBar.Type = output.TypeHTTPServer
	cBar.Label = "bar"

	conf := manager.NewResourceConfig()
	conf.ResourceOutputs = append(conf.ResourceOutputs, cFoo, cBar)

	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = mgr.AccessOutput(context.Background(), "foo", func(ow ioutput.Sync) {})
	require.NoError(t, err)

	err = mgr.AccessOutput(context.Background(), "bar", func(ow ioutput.Sync) {})
	require.NoError(t, err)

	err = mgr.AccessOutput(context.Background(), "baz", func(ow ioutput.Sync) {})
	assert.EqualError(t, err, "unable to locate resource: baz")
}

func TestManagerOutputListErrors(t *testing.T) {
	cFoo := output.NewConfig()
	cFoo.Label = "foo"

	cBar := output.NewConfig()
	cBar.Label = "foo"

	conf := manager.NewResourceConfig()
	conf.ResourceOutputs = append(conf.ResourceOutputs, cFoo, cBar)

	_, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "output resource label 'foo' collides with a previously defined resource")

	cEmpty := output.NewConfig()
	conf = manager.NewResourceConfig()
	conf.ResourceOutputs = append(conf.ResourceOutputs, cEmpty)

	_, err = manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "output resource has an empty label")
}

func TestManagerPipeErrors(t *testing.T) {
	conf := manager.NewResourceConfig()
	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err = mgr.GetPipe("does not exist"); err != component.ErrPipeNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, component.ErrPipeNotFound)
	}
}

func TestManagerPipeGetSet(t *testing.T) {
	conf := manager.NewResourceConfig()
	mgr, err := manager.NewV2(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	t1 := make(chan message.Transaction)
	t2 := make(chan message.Transaction)
	t3 := make(chan message.Transaction)

	mgr.SetPipe("foo", t1)
	mgr.SetPipe("bar", t3)

	var p <-chan message.Transaction
	if p, err = mgr.GetPipe("foo"); err != nil {
		t.Fatal(err)
	}
	if p != t1 {
		t.Error("Wrong transaction chan returned")
	}

	// Should be a noop
	mgr.UnsetPipe("foo", t2)
	if p, err = mgr.GetPipe("foo"); err != nil {
		t.Fatal(err)
	}
	if p != t1 {
		t.Error("Wrong transaction chan returned")
	}
	if p, err = mgr.GetPipe("bar"); err != nil {
		t.Fatal(err)
	}
	if p != t3 {
		t.Error("Wrong transaction chan returned")
	}

	mgr.UnsetPipe("foo", t1)
	if _, err = mgr.GetPipe("foo"); err != component.ErrPipeNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, component.ErrPipeNotFound)
	}

	// Back to before
	mgr.SetPipe("foo", t1)
	if p, err = mgr.GetPipe("foo"); err != nil {
		t.Fatal(err)
	}
	if p != t1 {
		t.Error("Wrong transaction chan returned")
	}

	// Now replace pipe
	mgr.SetPipe("foo", t2)
	if p, err = mgr.GetPipe("foo"); err != nil {
		t.Fatal(err)
	}
	if p != t2 {
		t.Error("Wrong transaction chan returned")
	}
	if p, err = mgr.GetPipe("bar"); err != nil {
		t.Fatal(err)
	}
	if p != t3 {
		t.Error("Wrong transaction chan returned")
	}
}

//------------------------------------------------------------------------------
