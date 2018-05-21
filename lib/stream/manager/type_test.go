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
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/stream"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

func harmlessConf() stream.Config {
	c := stream.NewConfig()
	c.Input.Type = "http_server"
	c.Output.Type = "http_server"
	return c
}

func TestTypeBasicOperations(t *testing.T) {
	mgr := New(
		OptSetLogger(log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
	)

	if err := mgr.Update("foo", harmlessConf(), time.Second); err == nil {
		t.Error("Expected error on empty update")
	}
	if _, err := mgr.Read("foo"); err == nil {
		t.Error("Expected error on empty read")
	}

	if err := mgr.Create("foo", harmlessConf()); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("foo", harmlessConf()); err == nil {
		t.Error("Expected error on duplicate create")
	}

	if info, err := mgr.Read("foo"); err != nil {
		t.Error(err)
	} else if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, harmlessConf(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	newConf := harmlessConf()
	newConf.Buffer.Type = "memory"

	if err := mgr.Update("foo", newConf, time.Second); err != nil {
		t.Error(err)
	}

	if info, err := mgr.Read("foo"); err != nil {
		t.Error(err)
	} else if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, newConf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	if err := mgr.Delete("foo", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Delete("foo", time.Second); err == nil {
		t.Error("Expected error on duplicate delete")
	}

	if err := mgr.Stop(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := types.ErrTypeClosed, mgr.Create("foo", harmlessConf()); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}

func TestTypeBasicClose(t *testing.T) {
	mgr := New(
		OptSetLogger(log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
	)

	conf := harmlessConf()
	conf.Output.Type = "scalability_protocols"

	if err := mgr.Create("foo", conf); err != nil {
		t.Fatal(err)
	}

	if err := mgr.Stop(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := types.ErrTypeClosed, mgr.Create("foo", harmlessConf()); act != exp {
		t.Errorf("Unexpected error: %v != %v", act, exp)
	}
}
