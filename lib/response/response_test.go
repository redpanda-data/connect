// Copyright (c) 2014 Ashley Jeffs
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

package response

import (
	"errors"
	"testing"
)

func TestError(t *testing.T) {
	err := errors.New("test error")
	res := NewError(err)

	if exp, act := err, res.Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", exp, act)
	}
	if res.SkipAck() {
		t.Error("Should not received skip ack on simple response")
	}
}

func TestNoack(t *testing.T) {
	res := NewNoack()

	if res.Error() == nil {
		t.Error("Should have received error on noack response")
	}
	if res.SkipAck() {
		t.Error("Should not have received skip ack on noack response")
	}
}

func TestAck(t *testing.T) {
	res := NewAck()

	if res.Error() != nil {
		t.Error(res.Error())
	}
	if res.SkipAck() {
		t.Error("Should not have received skip ack on ack response")
	}
}

func TestUnack(t *testing.T) {
	res := NewUnack()

	if res.Error() != nil {
		t.Error(res.Error())
	}
	if !res.SkipAck() {
		t.Error("Should have received skip ack on unack response")
	}
}
