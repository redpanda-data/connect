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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"nanomsg.org/go-mangos/protocol/pull"
	"nanomsg.org/go-mangos/transport/tcp"
)

func TestBrokerWithNanomsg(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = TypeNanomsg, TypeNanomsg
	scaleOne.Nanomsg.Bind, scaleTwo.Nanomsg.Bind = true, true
	scaleOne.Nanomsg.URLs = []string{"tcp://localhost:1241"}
	scaleTwo.Nanomsg.URLs = []string{"tcp://localhost:1242"}
	scaleOne.Nanomsg.SocketType, scaleTwo.Nanomsg.SocketType = "PUSH", "PUSH"

	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleTwo)

	s, err := NewBroker(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(sendChan); err != nil {
		t.Error(err)
		return
	}

	socketOne, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	socketTwo, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}

	socketOne.AddTransport(tcp.NewTransport())
	socketTwo.AddTransport(tcp.NewTransport())

	if err = socketOne.Dial("tcp://localhost:1241"); err != nil {
		t.Error(err)
		return
	}
	if err = socketTwo.Dial("tcp://localhost:1242"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		data, err := socketOne.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}

		data, err = socketTwo.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Error(res.Error())
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

func TestRoundRobinWithNanomsg(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()
	conf.Broker.Pattern = "round_robin"

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = TypeNanomsg, TypeNanomsg
	scaleOne.Nanomsg.Bind, scaleTwo.Nanomsg.Bind = true, true
	scaleOne.Nanomsg.URLs = []string{"tcp://localhost:1245"}
	scaleTwo.Nanomsg.URLs = []string{"tcp://localhost:1246"}
	scaleOne.Nanomsg.SocketType, scaleTwo.Nanomsg.SocketType = "PUSH", "PUSH"

	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, scaleTwo)

	s, err := NewBroker(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = s.Consume(sendChan); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socketOne, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	socketTwo, err := pull.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}

	socketOne.AddTransport(tcp.NewTransport())
	socketTwo.AddTransport(tcp.NewTransport())

	if err = socketOne.Dial("tcp://localhost:1245"); err != nil {
		t.Error(err)
		return
	}
	if err = socketTwo.Dial("tcp://localhost:1246"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		if i%2 == 0 {
			data, err := socketOne.Recv()
			if err != nil {
				t.Error(err)
				return
			}
			if res := string(data); res != testStr {
				t.Errorf("Wrong value on output: %v != %v", res, testStr)
			}
		} else {
			data, err := socketTwo.Recv()
			if err != nil {
				t.Error(err)
				return
			}
			if res := string(data); res != testStr {
				t.Errorf("Wrong value on output: %v != %v", res, testStr)
			}
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Error(res.Error())
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

func TestFanOutBroker(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_fan_out_broker_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo := NewConfig(), NewConfig()
	outOne.Type, outTwo.Type = TypeFiles, TypeFiles
	outOne.Files.Path = filepath.Join(dir, "one", "foo-${!count:1s}.txt")
	outTwo.Files.Path = filepath.Join(dir, "two", "bar-${!count:2s}.txt")

	procOne, procTwo := processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeText, processor.TypeText
	procOne.Text.Operator = "prepend"
	procOne.Text.Value = "one-"
	procTwo.Text.Operator = "prepend"
	procTwo.Text.Value = "two-"

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "fan_out"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, outTwo)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third",
	}
	expFiles := map[string]string{
		"./one/foo-1.txt": "one-first",
		"./one/foo-2.txt": "one-second",
		"./one/foo-3.txt": "one-third",
		"./two/bar-1.txt": "two-first",
		"./two/bar-2.txt": "two-second",
		"./two/bar-3.txt": "two-third",
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := ioutil.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}

func TestRoundRobinBroker(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_round_robin_broker_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo := NewConfig(), NewConfig()
	outOne.Type, outTwo.Type = TypeFiles, TypeFiles
	outOne.Files.Path = filepath.Join(dir, "one", "foo-${!count:rrfoo}.txt")
	outTwo.Files.Path = filepath.Join(dir, "two", "bar-${!count:rrbar}.txt")

	procOne, procTwo := processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeText, processor.TypeText
	procOne.Text.Operator = "prepend"
	procOne.Text.Value = "one-"
	procTwo.Text.Operator = "prepend"
	procTwo.Text.Value = "two-"

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "round_robin"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, outTwo)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string]string{
		"./one/foo-1.txt": "one-first",
		"./one/foo-2.txt": "one-third",
		"./two/bar-1.txt": "two-second",
		"./two/bar-2.txt": "two-fourth",
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := ioutil.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}

func TestGreedyBroker(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_broker_greedy_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo := NewConfig(), NewConfig()
	outOne.Type, outTwo.Type = TypeFiles, TypeFiles
	outOne.Files.Path = filepath.Join(dir, "one", "foo-${!count:gfoo}.txt")
	outTwo.Files.Path = filepath.Join(dir, "two", "bar-${!count:gbar}.txt")

	procOne, procTwo := processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeText, processor.TypeText
	procOne.Text.Operator = "prepend"
	procOne.Text.Value = "one-"
	procTwo.Text.Operator = "prepend"
	procTwo.Text.Value = "two-"

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	procOne, procTwo = processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type = processor.TypeSleep, processor.TypeSleep
	procOne.Sleep.Duration = "50ms"
	procTwo.Sleep.Duration = "50ms"

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "greedy"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, outTwo)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string][2]string{
		"./one/foo-1.txt": {"one-first", "one-second"},
		"./one/foo-2.txt": {"one-third", "one-fourth"},
		"./two/bar-1.txt": {"two-first", "two-second"},
		"./two/bar-2.txt": {"two-third", "two-fourth"},
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := ioutil.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp[0] != act && exp[1] != act {
			t.Errorf("Wrong contents for file '%v': %v != (%v || %v)", k, act, exp[0], exp[1])
		}
	}
}

func TestTryBroker(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_try_broker_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	outOne, outTwo, outThree := NewConfig(), NewConfig(), NewConfig()
	outOne.Type, outTwo.Type, outThree.Type = TypeHTTPClient, TypeFiles, TypeFile
	outOne.HTTPClient.URL = "http://localhost:11111111/badurl"
	outOne.HTTPClient.NumRetries = 1
	outOne.HTTPClient.Retry = "1ms"
	outTwo.Files.Path = filepath.Join(dir, "two", "bar-${!count:tfoo}-${!count:tbar}.txt")
	outThree.File.Path = "/dev/null"

	procOne, procTwo, procThree := processor.NewConfig(), processor.NewConfig(), processor.NewConfig()
	procOne.Type, procTwo.Type, procThree.Type = processor.TypeText, processor.TypeText, processor.TypeText
	procOne.Text.Operator = "prepend"
	procOne.Text.Value = "this-should-never-appear ${!count:tfoo}"
	procTwo.Text.Operator = "prepend"
	procTwo.Text.Value = "two-"
	procThree.Text.Operator = "prepend"
	procThree.Text.Value = "this-should-never-appear ${!count:tbar}"

	outOne.Processors = append(outOne.Processors, procOne)
	outTwo.Processors = append(outTwo.Processors, procTwo)
	outThree.Processors = append(outThree.Processors, procThree)

	conf := NewConfig()
	conf.Type = TypeBroker
	conf.Broker.Pattern = "try"
	conf.Broker.Outputs = append(conf.Broker.Outputs, outOne)
	conf.Broker.Outputs = append(conf.Broker.Outputs, outTwo)
	conf.Broker.Outputs = append(conf.Broker.Outputs, outThree)

	s, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string]string{
		"./two/bar-2-1.txt": "two-first",
		"./two/bar-4-2.txt": "two-second",
		"./two/bar-6-3.txt": "two-third",
		"./two/bar-8-4.txt": "two-fourth",
	}

	for _, input := range inputs {
		testMsg := message.New([][]byte{[]byte(input)})
		select {
		case sendChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Fatal(res.Error())
			}
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := ioutil.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}
