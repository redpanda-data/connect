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

package input

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/go-mangos/mangos/protocol/push"
	"github.com/go-mangos/mangos/transport/tcp"
)

func TestFanInConfigDefaults(t *testing.T) {
	testConf := []byte(`{
		"type": "fan_in",
		"fan_in": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"timeout_ms": 1
					}
				},
				{
					"type": "http_server",
					"http_server": {
						"address": "address:2",
						"path": "/2"
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	inputConfs, err := parseInputConfsWithDefaults(conf.FanIn)
	if err != nil {
		t.Error(err)
		return
	}

	if exp, actual := 2, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:2", inputConfs[1].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "/post", inputConfs[0].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[1].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := int64(1), inputConfs[0].HTTPServer.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(5000), inputConfs[1].HTTPServer.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestFanInConfigDitto(t *testing.T) {
	testConf := []byte(`{
		"type": "fan_in",
		"fan_in": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"path": "/1"
					}
				},
				{
					"type": "ditto",
					"http_server": {
						"path": "/2"
					}
				},
				{
					"type": "ditto",
					"http_server": {
						"path": "/3"
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	inputConfs, err := parseInputConfsWithDefaults(conf.FanIn)
	if err != nil {
		t.Error(err)
		return
	}

	if exp, actual := 3, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[1].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[2].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "/1", inputConfs[0].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[1].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/3", inputConfs[2].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestFanInConfigDittoMulti(t *testing.T) {
	testConf := []byte(`{
		"type": "fan_in",
		"fan_in": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"path": "/1"
					}
				},
				{
					"type": "ditto_2",
					"http_server": {
						"path": "/2"
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	inputConfs, err := parseInputConfsWithDefaults(conf.FanIn)
	if err != nil {
		t.Error(err)
		return
	}

	if exp, actual := 3, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[1].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[2].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "/1", inputConfs[0].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[1].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[2].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestFanInWithScaleProto(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = "scalability_protocols", "scalability_protocols"
	scaleOne.ScaleProto.Bind, scaleTwo.ScaleProto.Bind = true, true
	scaleOne.ScaleProto.Addresses = []string{"tcp://localhost:1247"}
	scaleTwo.ScaleProto.Addresses = []string{"tcp://localhost:1248"}
	scaleOne.ScaleProto.SocketType, scaleTwo.ScaleProto.SocketType = "PULL", "PULL"
	scaleOne.ScaleProto.PollTimeoutMS, scaleTwo.ScaleProto.PollTimeoutMS = 100, 100

	conf.FanIn.Inputs = append(conf.FanIn.Inputs, scaleOne)
	conf.FanIn.Inputs = append(conf.FanIn.Inputs, scaleTwo)

	s, err := NewFanIn(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = s.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		s.CloseAsync()
		if err := s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socketOne, err := push.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}
	socketTwo, err := push.NewSocket()
	if err != nil {
		t.Error(err)
		return
	}

	socketOne.AddTransport(tcp.NewTransport())
	socketTwo.AddTransport(tcp.NewTransport())

	if err = socketOne.Dial("tcp://localhost:1247"); err != nil {
		t.Error(err)
		return
	}
	if err = socketTwo.Dial("tcp://localhost:1248"); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("testOne%v", i)
		if i%2 == 0 {
			if err = socketOne.Send([]byte(testStr)); err != nil {
				t.Error(err)
				return
			}
		} else {
			if err = socketOne.Send([]byte(testStr)); err != nil {
				t.Error(err)
				return
			}
		}
		select {
		case resMsg := <-s.MessageChan():
			if res := string(resMsg.Parts[0]); res != testStr {
				t.Errorf("Wrong result, %v != %v", res, testStr)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}
}
