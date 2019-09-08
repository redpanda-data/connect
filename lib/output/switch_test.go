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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestSwitchInterfaces(t *testing.T) {
	f := &Switch{}
	if types.Consumer(f) == nil {
		t.Errorf("Switch: nil types.Consumer")
	}
	if types.Closable(f) == nil {
		t.Errorf("Switch: nil types.Closable")
	}
}

//------------------------------------------------------------------------------

func newSwitch(conf Config, mockOutputs []*MockOutputType) (*Switch, error) {
	conf.Type = "switch"
	genType, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		return nil, err
	}

	rType, ok := genType.(*Switch)
	if !ok {
		return nil, fmt.Errorf("failed to cast: %T", genType)
	}

	for i := 0; i < len(mockOutputs); i++ {
		close(rType.outputTsChans[i])
		rType.outputs[i] = mockOutputs[i]
		rType.outputTsChans[i] = make(chan types.Transaction)
		mockOutputs[i].Consume(rType.outputTsChans[i])
	}
	return rType, nil
}

//------------------------------------------------------------------------------

func TestSwitchNoConditions(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	conf := NewConfig()
	mockOutputs := []*MockOutputType{}
	for i := 0; i < nOutputs; i++ {
		conf.Switch.Outputs = append(conf.Switch.Outputs, NewSwitchConfigOutput())
		conf.Switch.Outputs[i].Fallthrough = true
		mockOutputs = append(mockOutputs, &MockOutputType{})
	}

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
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

func TestSwitchNoRetries(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	conf := NewConfig()
	conf.Switch.RetryUntilSuccess = false
	mockOutputs := []*MockOutputType{}
	for i := 0; i < nOutputs; i++ {
		conf.Switch.Outputs = append(conf.Switch.Outputs, NewSwitchConfigOutput())
		conf.Switch.Outputs[i].Fallthrough = true
		mockOutputs = append(mockOutputs, &MockOutputType{})
	}

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
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
				t.Fatal("Timed out waiting for broker propagate")
			}
		}
		for j := 0; j < nOutputs; j++ {
			var res types.Response
			if j == 1 {
				res = response.NewError(errors.New("test"))
			} else {
				res = response.NewAck()
			}
			select {
			case resChanSlice[j] <- res:
			case <-time.After(time.Second):
				t.Fatal("Timed out responding to broker")
			}
		}
		select {
		case res := <-resChan:
			if res.Error() == nil {
				t.Error("Received nil error from broker")
			} else {
				if exp, act := "test", res.Error().Error(); exp != act {
					t.Errorf("Wrong error message from broker: %v != %v", act, exp)
				}
			}
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	s.CloseAsync()

	if err := s.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestSwitchWithConditions(t *testing.T) {
	nMsgs := 100

	mockOutputs := []*MockOutputType{{}, {}, {}}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		conf.Switch.Outputs = append(conf.Switch.Outputs, NewSwitchConfigOutput())
	}

	fooConfig := condition.NewConfig()
	fooConfig.Type = condition.TypeJMESPath
	fooConfig.JMESPath.Query = "foo == 'bar'"
	conf.Switch.Outputs[0].Condition = fooConfig

	barConfig := condition.NewConfig()
	barConfig.Type = condition.TypeJMESPath
	barConfig.JMESPath.Query = "foo == 'baz'"
	conf.Switch.Outputs[1].Condition = barConfig

	s, err := newSwitch(conf, mockOutputs)
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

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		closed := 0
		bar := `{"foo":"bar"}`
		baz := `{"foo":"baz"}`

	outputLoop:
		for closed < len(mockOutputs) {
			var ts types.Transaction
			var ok bool

			select {
			case ts, ok = <-mockOutputs[0].TChan:
				if !ok {
					closed++
					continue outputLoop
				}
				if act := string(ts.Payload.Get(0).Get()); act != bar {
					t.Errorf("Expected output 0 msgs to equal %s, got %s", bar, act)
				}
			case ts, ok = <-mockOutputs[1].TChan:
				if !ok {
					closed++
					continue outputLoop
				}
				if act := string(ts.Payload.Get(0).Get()); act != baz {
					t.Errorf("Expected output 1 msgs to equal %s, got %s", baz, act)
				}
			case ts, ok = <-mockOutputs[2].TChan:
				if !ok {
					closed++
					continue outputLoop
				}
				if act := string(ts.Payload.Get(0).Get()); act == bar || act == baz {
					t.Errorf("Expected output 2 msgs to not equal %s or %s, got %s", bar, baz, act)
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for output to propagate")
				break outputLoop
			}

			select {
			case ts.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to output")
				break outputLoop
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
	wg.Wait()
}

func TestSwitchNoMatch(t *testing.T) {
	mockOutputs := []*MockOutputType{{}, {}, {}}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		conf.Switch.Outputs = append(conf.Switch.Outputs, NewSwitchConfigOutput())
	}

	fooConfig := condition.NewConfig()
	fooConfig.Type = condition.TypeJMESPath
	fooConfig.JMESPath.Query = "foo == 'bar'"
	conf.Switch.Outputs[0].Condition = fooConfig

	barConfig := condition.NewConfig()
	barConfig.Type = condition.TypeJMESPath
	barConfig.JMESPath.Query = "foo == 'baz'"
	conf.Switch.Outputs[1].Condition = barConfig

	bazConfig := condition.NewConfig()
	bazConfig.Type = condition.TypeStatic
	bazConfig.Static = false
	conf.Switch.Outputs[2].Condition = bazConfig

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(readChan); err != nil {
		t.Fatal(err)
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

	mockOutputs := []*MockOutputType{{}, {}, {}}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		conf.Switch.Outputs = append(conf.Switch.Outputs, NewSwitchConfigOutput())
	}

	fooConfig := condition.NewConfig()
	fooConfig.Type = condition.TypeJMESPath
	fooConfig.JMESPath.Query = "foo == 'bar'"
	conf.Switch.Outputs[0].Condition = fooConfig

	barConfig := condition.NewConfig()
	barConfig.Type = condition.TypeJMESPath
	barConfig.JMESPath.Query = "foo == 'baz'"
	conf.Switch.Outputs[1].Condition = barConfig

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		closed := 0
		bar := `{"foo":"bar"}`
		baz := `{"foo":"baz"}`

	outputLoop:
		for closed < len(mockOutputs) {
			var ts types.Transaction
			var ok bool

			resChans := []chan<- types.Response{}
			for len(resChans) < 1 {
				select {
				case ts, ok = <-mockOutputs[0].TChan:
					if !ok {
						closed++
						continue outputLoop
					}
					if act := string(ts.Payload.Get(0).Get()); act != bar {
						t.Errorf("Expected output 0 msgs to equal %s, got %s", bar, act)
					}
				case ts, ok = <-mockOutputs[1].TChan:
					if !ok {
						closed++
						continue outputLoop
					}
					if act := string(ts.Payload.Get(0).Get()); act != baz {
						t.Errorf("Expected output 1 msgs to equal %s, got %s", baz, act)
					}
				case ts, ok = <-mockOutputs[2].TChan:
					if !ok {
						closed++
						continue outputLoop
					}
					t.Error("Unexpected msg received by output 3")
				case <-time.After(time.Second):
					t.Error("Timed out waiting for output to propagate")
					break outputLoop
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
					break outputLoop
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

	wg.Wait()
}

func TestSwitchAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	mockOutputs := []*MockOutputType{
		&mockOne, &mockTwo,
	}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		outConf := NewSwitchConfigOutput()
		outConf.Fallthrough = true
		conf.Switch.Outputs = append(conf.Switch.Outputs, outConf)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
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

func TestSwitchShutDownFromErrorResponse(t *testing.T) {
	mockOutputs := []*MockOutputType{{}, {}}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		outConf := NewSwitchConfigOutput()
		outConf.Fallthrough = true
		conf.Switch.Outputs = append(conf.Switch.Outputs, outConf)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
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
	case ts, open = <-mockOutputs[0].TChan:
		if !open {
			t.Error("Switch output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
	select {
	case _, open = <-mockOutputs[1].TChan:
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
	case _, open := <-mockOutputs[0].TChan:
		if open {
			t.Error("Switch output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestSwitchShutDownFromReceive(t *testing.T) {
	mockOutputs := []*MockOutputType{{}, {}}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		outConf := NewSwitchConfigOutput()
		outConf.Fallthrough = true
		conf.Switch.Outputs = append(conf.Switch.Outputs, outConf)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
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
	case _, open := <-mockOutputs[0].TChan:
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
	case _, open := <-mockOutputs[0].TChan:
		if open {
			t.Error("Switch output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestSwitchShutDownFromSend(t *testing.T) {
	mockOutputs := []*MockOutputType{{}, {}}

	conf := NewConfig()
	for i := 0; i < len(mockOutputs); i++ {
		outConf := NewSwitchConfigOutput()
		outConf.Fallthrough = true
		conf.Switch.Outputs = append(conf.Switch.Outputs, outConf)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	s, err := newSwitch(conf, mockOutputs)
	if err != nil {
		t.Fatal(err)
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

	s.CloseAsync()
	if err := s.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutputs[0].TChan:
		if open {
			t.Error("Switch output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

//------------------------------------------------------------------------------
