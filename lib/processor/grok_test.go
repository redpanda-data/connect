package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestGrokAllParts(t *testing.T) {
	conf := NewConfig()
	conf.Grok.Parts = []int{}
	conf.Grok.Patterns = []string{
		"%{WORD:first},%{INT:second:int}",
	}

	testLog := log.Noop()

	gSet, err := NewGrok(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.New([][]byte{
		[]byte(`foo,0`),
		[]byte(`foo,1`),
		[]byte(`foo,2`),
	})
	msgs, res := gSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}

	exp := [][]byte{
		[]byte(`{"first":"foo","second":0}`),
		[]byte(`{"first":"foo","second":1}`),
		[]byte(`{"first":"foo","second":2}`),
	}
	act := message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong output from grok: %s != %s", act, exp)
	}
}

func TestGrok(t *testing.T) {
	tLog := log.Noop()
	tStats := metrics.Noop()

	type gTest struct {
		name        string
		pattern     string
		input       string
		output      string
		definitions map[string]string
	}

	tests := []gTest{
		{
			name:    "Common apache parsing",
			pattern: "%{COMMONAPACHELOG}",
			input:   `127.0.0.1 - - [23/Apr/2014:22:58:32 +0200] "GET /index.php HTTP/1.1" 404 207`,
			output:  `{"auth":"-","bytes":"207","clientip":"127.0.0.1","httpversion":"1.1","ident":"-","request":"/index.php","response":"404","timestamp":"23/Apr/2014:22:58:32 +0200","verb":"GET"}`,
		},
		{
			name: "Test pattern definitions",
			definitions: map[string]string{
				"ACTION": "(pass|deny)",
			},
			input:   `pass connection from 127.0.0.1`,
			pattern: "%{ACTION:action} connection from %{IPV4:ipv4}",
			output:  `{"action":"pass","ipv4":"127.0.0.1"}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Grok.Parts = []int{0}
		conf.Grok.Patterns = []string{test.pattern}
		conf.Grok.PatternDefinitions = test.definitions

		gSet, err := NewGrok(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := message.New(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := gSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
