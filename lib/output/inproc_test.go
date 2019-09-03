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

package output

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestInproc(t *testing.T) {
	mgr, err := manager.New(manager.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := NewConfig()
	conf.Inproc = "foo"

	var ip Type
	if ip, err = NewInproc(conf, mgr, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}

	if _, err = mgr.GetPipe("foo"); err != types.ErrPipeNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrPipeNotFound)
	}

	tinchan := make(chan types.Transaction)
	if err = ip.Consume(tinchan); err != nil {
		t.Fatal(err)
	}

	select {
	case tinchan <- types.NewTransaction(nil, nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var toutchan <-chan types.Transaction
	if toutchan, err = mgr.GetPipe("foo"); err != nil {
		t.Error(err)
	}

	select {
	case <-toutchan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	ip.CloseAsync()
	if err = ip.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-toutchan:
		if open {
			t.Error("transaction chan not closed")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	if _, err = mgr.GetPipe("foo"); err != types.ErrPipeNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrPipeNotFound)
	}
}

//------------------------------------------------------------------------------
