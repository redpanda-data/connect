// Copyright (c) 2019 Ashley Jeffs
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

package roundtrip

import (
	"context"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestResultStore(t *testing.T) {
	impl := &resultStoreImpl{}
	ctx := context.WithValue(context.Background(), ResultStoreKey, impl)
	msg := message.New(nil)
	var p types.Part = message.NewPart([]byte("foo"))
	p = message.WithContext(ctx, p)
	msg.Append(p)
	msg.Append(message.NewPart([]byte("bar")))

	impl.Add(msg)
	results := impl.Get()
	if len(results) != 1 {
		t.Fatalf("Wrong count of result batches: %v", len(results))
	}
	if results[0].Len() != 2 {
		t.Fatalf("Wrong count of messages: %v", results[0].Len())
	}
	if exp, act := "foo", string(results[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(results[0].Get(1).Get()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if store := message.GetContext(results[0].Get(0)).Value(ResultStoreKey); store != nil {
		t.Error("Unexpected nested result store")
	}

	impl.Clear()
	if exp, act := len(impl.Get()), 0; exp != act {
		t.Errorf("Unexpected count of stored messages: %v != %v", act, exp)
	}
}
