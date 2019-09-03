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

package stream

import (
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestTypeConstruction(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Input.Nanomsg.PollTimeout = "100ms"
	conf.Output.Type = output.TypeNanomsg

	strm, err := New(conf) // nanomsg => nanomsg
	if err != nil {
		t.Fatal(err)
	}

	if strm.logger == nil {
		t.Error("nil logger")
	}

	if strm.stats == nil {
		t.Error("nil stats")
	}

	if strm.manager == nil {
		t.Error("nil manager")
	}

	if err = strm.Stop(time.Second * 10); err != nil {
		t.Error(err)
	}

	newStats := metrics.DudType{
		ID: 1,
	}
	newLogger := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	newMgr := types.DudMgr{
		ID: 2,
	}

	strm, err = New(conf, OptSetLogger(newLogger), OptSetStats(newStats), OptSetManager(newMgr))
	if err != nil {
		t.Fatal(err)
	}

	if strm.logger != newLogger {
		t.Error("wrong logger")
	}

	if strm.stats != newStats {
		t.Error("wrong stats")
	}

	if strm.manager != newMgr {
		t.Error("wrong manager")
	}
}

func TestTypeCloseGracefully(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Output.Type = output.TypeNanomsg

	strm, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopGracefully(time.Second); err != nil {
		t.Error(err)
	}

	conf.Buffer.Type = "memory"

	strm, err = New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopGracefully(time.Second); err != nil {
		t.Error(err)
	}

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopGracefully(time.Second); err != nil {
		t.Error(err)
	}
}

func TestTypeCloseOrdered(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Output.Type = output.TypeNanomsg

	strm, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopOrdered(time.Second); err != nil {
		t.Error(err)
	}

	conf.Buffer.Type = "memory"

	strm, err = New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopOrdered(time.Second); err != nil {
		t.Error(err)
	}

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopOrdered(time.Second); err != nil {
		t.Error(err)
	}
}

func TestTypeCloseUnordered(t *testing.T) {
	conf := NewConfig()
	conf.Input.Type = input.TypeNanomsg
	conf.Output.Type = input.TypeNanomsg

	strm, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopUnordered(time.Second); err != nil {
		t.Error(err)
	}

	conf.Buffer.Type = "memory"

	strm, err = New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopUnordered(time.Second); err != nil {
		t.Error(err)
	}

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if err = strm.stopUnordered(time.Second); err != nil {
		t.Error(err)
	}
}
