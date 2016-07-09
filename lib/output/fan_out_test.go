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

package output

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

func TestFanOutConfigDefaults(t *testing.T) {
	testConf := []byte(`{
		"type": "fan_out",
		"fan_out": {
			"outputs": [
				{
					"type": "http_client",
					"http_client": {
						"url": "address:1",
						"timeout_ms": 1
					}
				},
				{
					"type": "http_client",
					"http_client": {
						"url": "address:2",
						"retry_period_ms": 2
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

	outputConfs, err := parseOutputConfsWithDefaults(conf.FanOut)
	if err != nil {
		t.Error(err)
		return
	}

	if exp, actual := 2, len(outputConfs); exp != actual {
		t.Errorf("unexpected number of output configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_client", outputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_client", outputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", outputConfs[0].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:2", outputConfs[1].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := int64(1), outputConfs[0].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(5000), outputConfs[1].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := int64(1000), outputConfs[0].HTTPClient.RetryMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(2), outputConfs[1].HTTPClient.RetryMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestFanOutWithScaleProto(t *testing.T) {
	nTestLoops := 1000

	conf := NewConfig()

	scaleOne, scaleTwo := NewConfig(), NewConfig()
	scaleOne.Type, scaleTwo.Type = "scalability_protocols", "scalability_protocols"
	scaleOne.ScaleProto.Bind, scaleTwo.ScaleProto.Bind = true, true
	scaleOne.ScaleProto.Address = "tcp://localhost:1241"
	scaleTwo.ScaleProto.Address = "tcp://localhost:1242"
	scaleOne.ScaleProto.SocketType, scaleTwo.ScaleProto.SocketType = "PUSH", "PUSH"

	conf.FanOut.Outputs = append(conf.FanOut.Outputs, scaleOne)
	conf.FanOut.Outputs = append(conf.FanOut.Outputs, scaleTwo)

	s, err := NewFanOut(conf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	sendChan := make(chan types.Message)

	if err = s.StartReceiving(sendChan); err != nil {
		t.Error(err)
		return
	}

	defer s.CloseAsync()
	defer s.WaitForClose(time.Second)

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
		testMsg := types.Message{Parts: [][]byte{[]byte(testStr)}}

		select {
		case sendChan <- testMsg:
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
		case res := <-s.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.Message{Parts: [][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		}}

		select {
		case sendChan <- testMsg:
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}

		data, err := socketOne.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		msg, err := types.FromBytes(data)
		if err != nil {
			t.Error(err)
			return
		}
		if exp, actual := 2, len(msg.Parts); exp != actual {
			t.Errorf("Unexpected message parts received: %v != %v", exp, actual)
			return
		}
		if exp, actual := testStr+"PART-A", string(msg.Parts[0]); exp != actual {
			t.Errorf("Unexpected message received: %v != %v", exp, actual)
			return
		}
		if exp, actual := testStr+"PART-B", string(msg.Parts[1]); exp != actual {
			t.Errorf("Unexpected message received: %v != %v", exp, actual)
			return
		}

		data, err = socketTwo.Recv()
		if err != nil {
			t.Error(err)
			return
		}
		msg, err = types.FromBytes(data)
		if err != nil {
			t.Error(err)
			return
		}
		if exp, actual := 2, len(msg.Parts); exp != actual {
			t.Errorf("Unexpected message parts received: %v != %v", exp, actual)
			return
		}
		if exp, actual := testStr+"PART-A", string(msg.Parts[0]); exp != actual {
			t.Errorf("Unexpected message received: %v != %v", exp, actual)
			return
		}
		if exp, actual := testStr+"PART-B", string(msg.Parts[1]); exp != actual {
			t.Errorf("Unexpected message received: %v != %v", exp, actual)
			return
		}

		select {
		case res := <-s.ResponseChan():
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
