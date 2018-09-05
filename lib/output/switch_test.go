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

package output

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

var (
	fallthroughConfig = switchConfigOutputs{Fallthrough: true}
)

func TestSwitchInterfaces(t *testing.T) {
	f := &Switch{}
	if types.Consumer(f) == nil {
		t.Errorf("FanOut: nil types.Consumer")
	}
	if types.Closable(f) == nil {
		t.Errorf("FanOut: nil types.Closable")
	}
}

//------------------------------------------------------------------------------

func TestSwitchNoConditions(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []types.Output{}
	confs := []switchConfigOutputs{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs = append(outputs, mockOutputs[i])
		confs = append(confs, fallthroughConfig)
	}

	s, err := newSwitch(outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		resChanSlice := []chan<- types.Response{}
		for j := 0; j < nOutputs; j++ {
			var ts types.Transaction
			select {
			case ts = <-mockOutputs[j].TChan:
				if string(ts.Payload.Get(0).Get()) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
				resChanSlice = append(resChanSlice, ts.ResponseChan)
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
		}
		for j := 0; j < nOutputs; j++ {
			select {
			case resChanSlice[j] <- response.NewAck():
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to broker")
				return
			}
		}
		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Errorf("Received unexpected errors from broker: %v", res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	s.CloseAsync()

	if err := s.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestSwitchWithConditions(t *testing.T) {
	nMsgs := 100

	mockOutputs := []*MockOutputType{&MockOutputType{}, &MockOutputType{}, &MockOutputType{}}
	outputs := []types.Output{mockOutputs[0], mockOutputs[1], mockOutputs[2]}
	confs := make([]switchConfigOutputs, len(outputs))

	fooConfig := condition.NewConfig()
	fooConfig.Type = condition.TypeJMESPath
	fooConfig.JMESPath.Query = "foo == 'bar'"
	fooCondition, _ := condition.NewJMESPath(fooConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[0] = switchConfigOutputs{
		cond: fooCondition,
	}

	barConfig := condition.NewConfig()
	barConfig.Type = condition.TypeJMESPath
	barConfig.JMESPath.Query = "foo == 'baz'"
	barCondition, _ := condition.NewJMESPath(barConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[1] = switchConfigOutputs{
		cond: barCondition,
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	go func() {
		closed := 0
		bar := `{"foo":"bar"}`
		baz := `{"foo":"baz"}`
		for closed < len(outputs) {
			var ts types.Transaction
			var ok bool

			select {
			case ts, ok = <-mockOutputs[0].TChan:
				if !ok {
					closed++
					break
				}
				if act := string(ts.Payload.Get(0).Get()); act != bar {
					t.Errorf("Expected output 0 msgs to equal %s, got %s", bar, act)
				}
			case ts, ok = <-mockOutputs[1].TChan:
				if !ok {
					closed++
					break
				}
				if act := string(ts.Payload.Get(0).Get()); act != baz {
					t.Errorf("Expected output 1 msgs to equal %s, got %s", baz, act)
				}
			case ts, ok = <-mockOutputs[2].TChan:
				if !ok {
					closed++
					break
				}
				if act := string(ts.Payload.Get(0).Get()); act == bar || act == baz {
					t.Errorf("Expected output 2 msgs to not equal %s or %s, got %s", bar, baz, act)
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for output to propagate")
				break
			}

			select {
			case ts.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to output")
				break
			}
		}
	}()

	for i := 0; i < nMsgs; i++ {
		foo := "bar"
		if i%3 == 0 {
			foo = "qux"
		} else if i%2 == 0 {
			foo = "baz"
		}
		content := [][]byte{[]byte(fmt.Sprintf("{\"foo\":\"%s\"}", foo))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for output send")
			return
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Errorf("Received unexpected errors from output: %v", res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to output")
			return
		}
	}

	s.CloseAsync()

	if err := s.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestSwitchNoMatch(t *testing.T) {
	mockOutputs := []*MockOutputType{&MockOutputType{}, &MockOutputType{}, &MockOutputType{}}
	outputs := []types.Output{mockOutputs[0], mockOutputs[1], mockOutputs[2]}
	confs := make([]switchConfigOutputs, len(outputs))

	fooConfig := condition.NewConfig()
	fooConfig.Type = condition.TypeJMESPath
	fooConfig.JMESPath.Query = "foo == 'bar'"
	fooCondition, _ := condition.NewJMESPath(fooConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[0] = switchConfigOutputs{
		cond: fooCondition,
	}

	barConfig := condition.NewConfig()
	barConfig.Type = condition.TypeJMESPath
	barConfig.JMESPath.Query = "foo == 'baz'"
	barCondition, _ := condition.NewJMESPath(barConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[1] = switchConfigOutputs{
		cond: barCondition,
	}

	bazConfig := condition.NewConfig()
	bazConfig.Type = condition.TypeStatic
	bazConfig.Static = false
	bazCondition, _ := condition.NewStatic(bazConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[2] = switchConfigOutputs{
		cond: bazCondition,
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	msg := message.New([][]byte{[]byte(`{"foo":"qux"}`)})
	select {
	case readChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for output send")
		return
	}

	select {
	case res := <-resChan:
		if err := res.Error(); err != nil {
			t.Errorf("Expected error from output to equal nil, got %v", err)
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out responding to output")
		return
	}

	s.CloseAsync()

	if err := s.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestSwitchWithConditionsNoFallthrough(t *testing.T) {
	nMsgs := 100

	mockOutputs := []*MockOutputType{&MockOutputType{}, &MockOutputType{}, &MockOutputType{}}
	outputs := []types.Output{mockOutputs[0], mockOutputs[1], mockOutputs[2]}
	confs := make([]switchConfigOutputs, len(outputs))

	fooConfig := condition.NewConfig()
	fooConfig.Type = condition.TypeJMESPath
	fooConfig.JMESPath.Query = "foo == 'bar'"
	fooCondition, _ := condition.NewJMESPath(fooConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[0] = switchConfigOutputs{
		cond: fooCondition,
	}

	barConfig := condition.NewConfig()
	barConfig.Type = condition.TypeJMESPath
	barConfig.JMESPath.Query = "foo == 'baz'"
	barCondition, _ := condition.NewJMESPath(barConfig, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	confs[1] = switchConfigOutputs{
		cond: barCondition,
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	go func() {
		closed := 0
		bar := `{"foo":"bar"}`
		baz := `{"foo":"baz"}`
		for closed < len(outputs) {
			var ts types.Transaction
			var ok bool

			resChans := []chan<- types.Response{}
			for len(resChans) < 1 {
				select {
				case ts, ok = <-mockOutputs[0].TChan:
					if !ok {
						closed++
						break
					}
					if act := string(ts.Payload.Get(0).Get()); act != bar {
						t.Errorf("Expected output 0 msgs to equal %s, got %s", bar, act)
					}
				case ts, ok = <-mockOutputs[1].TChan:
					if !ok {
						closed++
						break
					}
					if act := string(ts.Payload.Get(0).Get()); act != baz {
						t.Errorf("Expected output 1 msgs to equal %s, got %s", baz, act)
					}
				case ts, ok = <-mockOutputs[2].TChan:
					if !ok {
						closed++
						break
					}
					t.Error("Unexpected msg received by output 3")
				case <-time.After(time.Second):
					t.Error("Timed out waiting for output to propagate")
					break
				}
				if ts.ResponseChan != nil {
					resChans = append(resChans, ts.ResponseChan)
				}
			}

			for i := 0; i < len(resChans); i++ {
				select {
				case resChans[i] <- response.NewAck():
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to output")
					break
				}
			}
		}
	}()

	for i := 0; i < nMsgs; i++ {
		foo := "bar"
		if i%2 == 0 {
			foo = "baz"
		}
		content := [][]byte{[]byte(fmt.Sprintf("{\"foo\":\"%s\"}", foo))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for output send")
			return
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Errorf("Received unexpected errors from output: %v", res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to output")
			return
		}
	}

	s.CloseAsync()

	if err := s.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
func TestSwitchAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := []types.Output{&mockOne, &mockTwo}
	confs := []switchConfigOutputs{fallthroughConfig, fallthroughConfig}
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err == nil {
		t.Error("Expected error on duplicate receive call")
	}

	select {
	case readChan <- types.NewTransaction(message.New([][]byte{[]byte("hello world")}), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for output send")
		return
	}
	var ts1, ts2 types.Transaction
	select {
	case ts1 = <-mockOne.TChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
		return
	}
	select {
	case ts2 = <-mockTwo.TChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
		return
	}
	select {
	case ts1.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out responding to output")
		return
	}
	select {
	case ts2.ResponseChan <- response.NewError(errors.New("this is a test")):
	case <-time.After(time.Second):
		t.Error("Timed out responding to output")
		return
	}
	select {
	case ts1 = <-mockOne.TChan:
		t.Error("Received duplicate message to mockOne")
	case ts2 = <-mockTwo.TChan:
	case <-resChan:
		t.Error("Received premature response from output")
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockTwo")
		return
	}
	select {
	case ts2.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out responding to output")
		return
	}
	select {
	case res := <-resChan:
		if res.Error() != nil {
			t.Errorf("Fan out returned error %v", res.Error())
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out responding to output")
		return
	}

	close(readChan)

	if err := s.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestFanOutShutDownFromErrorResponse(t *testing.T) {
	outputs := []types.Output{}
	confs := []switchConfigOutputs{fallthroughConfig}
	mockOutput := &MockOutputType{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	var ts types.Transaction
	var open bool
	select {
	case ts, open = <-mockOutput.TChan:
		if !open {
			t.Error("Switch output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	select {
	case ts.ResponseChan <- response.NewError(errors.New("test")):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for res send")
	}

	s.CloseAsync()
	if err := s.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("Switch output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromReceive(t *testing.T) {
	outputs := []types.Output{}
	confs := []switchConfigOutputs{fallthroughConfig}
	mockOutput := &MockOutputType{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = s.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutput.TChan:
		if !open {
			t.Error("Switch output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	s.CloseAsync()
	if err := s.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("Switch output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromSend(t *testing.T) {
	outputs := []types.Output{}
	confs := []switchConfigOutputs{fallthroughConfig}
	mockOutput := &MockOutputType{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("Switch output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

//------------------------------------------------------------------------------

func BenchmarkBasicFanOut(b *testing.B) {
	nOutputs, nMsgs := 3, b.N

	outputs := []types.Output{}
	confs := []switchConfigOutputs{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs = append(outputs, mockOutputs[i])
		confs = append(confs, fallthroughConfig)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := newSwitch(
		outputs, confs, nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		b.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		b.Error(err)
		return
	}

	content := [][]byte{[]byte("hello world")}
	rChanSlice := make([]chan<- types.Response, nOutputs)

	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- types.NewTransaction(message.New(content), resChan)
		for j := 0; j < nOutputs; j++ {
			ts := <-mockOutputs[j].TChan
			rChanSlice[i] = ts.ResponseChan
		}
		for j := 0; j < nOutputs; j++ {
			rChanSlice[j] <- response.NewAck()
		}
		res := <-resChan
		if res.Error() != nil {
			b.Errorf("Received unexpected errors from broker: %v", res.Error())
		}
	}

	b.StopTimer()
}

//------------------------------------------------------------------------------
