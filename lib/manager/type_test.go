// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package manager

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestManagerCache(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	conf := NewConfig()
	conf.Caches["foo"] = cache.NewConfig()
	conf.Caches["bar"] = cache.NewConfig()

	mgr, err := New(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetCache("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetCache("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetCache("baz"); err != types.ErrCacheNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrCacheNotFound)
	}
}

func TestManagerBadCache(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	conf := NewConfig()
	badConf := cache.NewConfig()
	badConf.Type = "notexist"
	conf.Caches["bad"] = badConf

	if _, err := New(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Fatal("Expected error from bad cache")
	}
}

func TestManagerRateLimit(t *testing.T) {
	conf := NewConfig()
	conf.RateLimits["foo"] = ratelimit.NewConfig()
	conf.RateLimits["bar"] = ratelimit.NewConfig()

	mgr, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetRateLimit("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetRateLimit("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetRateLimit("baz"); err != types.ErrRateLimitNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrRateLimitNotFound)
	}
}

func TestManagerBadRateLimit(t *testing.T) {
	conf := NewConfig()
	badConf := ratelimit.NewConfig()
	badConf.Type = "notexist"
	conf.RateLimits["bad"] = badConf

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Fatal("Expected error from bad rate limit")
	}
}

func TestManagerCondition(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	conf := NewConfig()
	conf.Conditions["foo"] = condition.NewConfig()
	conf.Conditions["bar"] = condition.NewConfig()

	mgr, err := New(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetCondition("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetCondition("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetCondition("baz"); err != types.ErrConditionNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrConditionNotFound)
	}
}

func TestManagerProcessor(t *testing.T) {
	conf := NewConfig()
	conf.Processors["foo"] = processor.NewConfig()
	conf.Processors["bar"] = processor.NewConfig()

	mgr, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetProcessor("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetProcessor("bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetProcessor("baz"); err != types.ErrProcessorNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrProcessorNotFound)
	}
}

func TestManagerConditionRecursion(t *testing.T) {
	t.Skip("Not yet implemented")

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	conf := NewConfig()

	fooConf := condition.NewConfig()
	fooConf.Type = "resource"
	fooConf.Resource = "bar"
	conf.Conditions["foo"] = fooConf

	barConf := condition.NewConfig()
	barConf.Type = "resource"
	barConf.Resource = "foo"
	conf.Conditions["bar"] = barConf

	if _, err := New(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from recursive conditions")
	}
}

func TestManagerBadCondition(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	conf := NewConfig()
	badConf := condition.NewConfig()
	badConf.Type = "notexist"
	conf.Conditions["bad"] = badConf

	if _, err := New(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Fatal("Expected error from bad condition")
	}
}

func TestManagerPipeErrors(t *testing.T) {
	conf := NewConfig()
	mgr, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if _, err = mgr.GetPipe("does not exist"); err != types.ErrPipeNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrPipeNotFound)
	}
}

func TestManagerPipeGetSet(t *testing.T) {
	conf := NewConfig()
	mgr, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	t1 := make(chan types.Transaction)
	t2 := make(chan types.Transaction)
	t3 := make(chan types.Transaction)

	mgr.SetPipe("foo", t1)
	mgr.SetPipe("bar", t3)

	var p <-chan types.Transaction
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
	if _, err = mgr.GetPipe("foo"); err != types.ErrPipeNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrPipeNotFound)
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
