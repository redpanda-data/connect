/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package broker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/buffer"
)

//--------------------------------------------------------------------------------------------------

func TestBasicPropagatorInterface(t *testing.T) {
	mock1 := &buffer.MockType{
		Errors: make(chan []error),
	}
	buffers := []buffer.Type{mock1}
	errProp := NewErrPropagator(buffers)

	expected := errors.New("test")
	mock1.Errors <- []error{expected}

	select {
	case errMap := <-errProp.OutputChan():
		if len(errMap) == 0 {
			t.Errorf("Expected an error, received none")
			return
		}
		if actual, ok := errMap[0]; !ok {
			t.Errorf("No error for element 0")
		} else if len(actual) != 1 || actual[0] != expected {
			t.Errorf("Wrong error, %v != %v", actual, []error{expected})
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for error")
	}
}

//--------------------------------------------------------------------------------------------------

func TestPropagatorSwappedBuffers(t *testing.T) {
	mock1 := &buffer.MockType{
		Errors: make(chan []error),
	}
	buffers := []buffer.Type{mock1}
	errProp := NewErrPropagator([]buffer.Type{})

	errProp.SetBuffers(buffers)

	expected := errors.New("test")
	mock1.Errors <- []error{expected}

	select {
	case errMap := <-errProp.OutputChan():
		if len(errMap) == 0 {
			t.Errorf("Expected an error, received none")
			return
		}
		if actual, ok := errMap[0]; !ok {
			t.Errorf("No error for element 0")
		} else if len(actual) != 1 || actual[0] != expected {
			t.Errorf("Wrong error, %v != %v", actual, []error{expected})
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for error")
	}
}

//--------------------------------------------------------------------------------------------------

func TestPropagatorMultiBuffers(t *testing.T) {
	chans := []chan []error{
		make(chan []error),
		make(chan []error),
		make(chan []error),
		make(chan []error),
		make(chan []error),
	}

	buffers := []buffer.Type{}
	for _, c := range chans {
		buffers = append(buffers, &buffer.MockType{Errors: c})
	}

	errProp := NewErrPropagator(buffers)

	for i, c := range chans {
		errs := make([]error, i)
		for j := range errs {
			errs[j] = fmt.Errorf("%v", i)
		}
		select {
		case c <- errs:
		case <-time.After(time.Second):
			t.Errorf("Timed out pushing errors to channel %v", i)
		}
	}

	select {
	case errMap := <-errProp.OutputChan():
		if expected, actual := len(chans), len(errMap); expected != actual {
			t.Errorf("Unexpected number of errors returned, %v != %v", actual, expected)
			return
		}
		for i, errs := range errMap {
			if len(errs) != i {
				t.Errorf("Wrong number of errors from chan %v, %v != %v", i, len(errs), i)
			} else {
				for j := range errs {
					if expected, actual := fmt.Sprintf("%v", i), errs[j].Error(); expected != actual {
						t.Errorf("Wrong error value for channel %v index %v, %v != %v", i, j, actual, expected)
					}
				}
			}
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for error")
	}
}

//--------------------------------------------------------------------------------------------------
