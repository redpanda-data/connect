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
	"encoding/json"
	"testing"
)

func TestConfigSanitised(t *testing.T) {
	var err error
	var dat interface{}
	var actBytes []byte

	c := NewConfig()
	c.Input.Processors = nil
	c.Output.Processors = nil

	exp := `{` +
		`"input":{"type":"stdin","stdin":{"delimiter":"","max_buffer":1000000,"multipart":false}},` +
		`"buffer":{"type":"none","none":{}},` +
		`"pipeline":{"processors":[],"threads":1},` +
		`"output":{"type":"stdout","stdout":{"delimiter":""}}` +
		`}`

	if dat, err = c.Sanitised(); err != nil {
		t.Fatal(err)
	}
	if actBytes, err = json.Marshal(dat); err != nil {
		t.Fatal(err)
	}
	if act := string(actBytes); exp != act {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}

	c = NewConfig()
	c.Input.Processors = nil
	c.Input.Type = "file"
	c.Output.Processors = nil
	c.Output.Type = "kafka"

	exp = `{` +
		`"input":{"type":"file","file":{"delimiter":"","max_buffer":1000000,"multipart":false,"path":""}},` +
		`"buffer":{"type":"none","none":{}},` +
		`"pipeline":{"processors":[],"threads":1},` +
		`"output":{"type":"kafka","kafka":{"ack_replicas":false,"addresses":["localhost:9092"],"client_id":"benthos_kafka_output","compression":"none","key":"","max_msg_bytes":1000000,"round_robin_partitions":false,"skip_cert_verify":false,"target_version":"1.0.0","timeout_ms":5000,"tls_enable":false,"topic":"benthos_stream"}}` +
		`}`

	if dat, err = c.Sanitised(); err != nil {
		t.Fatal(err)
	}
	if actBytes, err = json.Marshal(dat); err != nil {
		t.Fatal(err)
	}
	if act := string(actBytes); exp != act {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}
}
