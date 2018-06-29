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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/processor"
)

func TestSanitise(t *testing.T) {
	var actObj interface{}
	var act []byte
	var err error

	exp := `{` +
		`"type":"amqp",` +
		`"amqp":{` +
		`"exchange":"benthos-exchange",` +
		`"exchange_type":"direct",` +
		`"immediate":false,` +
		`"key":"benthos-key",` +
		`"mandatory":false,` +
		`"persistent":false,` +
		`"url":"amqp://guest:guest@localhost:5672/"` +
		`}` +
		`}`

	conf := NewConfig()
	conf.Type = "amqp"
	conf.Processors = nil

	if actObj, err = SanitiseConfig(conf); err != nil {
		t.Fatal(err)
	}
	if act, err = json.Marshal(actObj); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(string(act), exp) {
		t.Errorf("Wrong sanitised output: %s != %v", act, exp)
	}

	exp = `{` +
		`"type":"amqp",` +
		`"amqp":{` +
		`"exchange":"benthos-exchange",` +
		`"exchange_type":"direct",` +
		`"immediate":false,` +
		`"key":"benthos-key",` +
		`"mandatory":false,` +
		`"persistent":false,` +
		`"url":"amqp://guest:guest@localhost:5672/"` +
		`},` +
		`"processors":[` +
		`{` +
		`"type":"combine",` +
		`"combine":{` +
		`"parts":2` +
		`}` +
		`},` +
		`{` +
		`"type":"archive",` +
		`"archive":{` +
		`"format":"binary",` +
		`"path":"nope"` +
		`}` +
		`}` +
		`]` +
		`}`

	proc := processor.NewConfig()
	proc.Type = "combine"
	conf.Processors = append(conf.Processors, proc)

	proc = processor.NewConfig()
	proc.Type = "archive"
	proc.Archive.Path = "nope"
	conf.Processors = append(conf.Processors, proc)

	if actObj, err = SanitiseConfig(conf); err != nil {
		t.Fatal(err)
	}
	if act, err = json.Marshal(actObj); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(string(act), exp) {
		t.Errorf("Wrong sanitised output: %s != %v", act, exp)
	}
}
