package pure_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestGrokAllParts(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "grok"
	conf.Grok.Expressions = []string{
		"%{WORD:first},%{INT:second:int}",
	}

	gSet, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.QuickBatch([][]byte{
		[]byte(`foo,0`),
		[]byte(`foo,1`),
		[]byte(`foo,2`),
	})
	msgs, res := gSet.ProcessBatch(context.Background(), msgIn)
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
		{
			name:    "Test dot path in name definition",
			input:   `foo 5 bazes from 192.0.1.11`,
			pattern: "%{WORD:nested.name} %{INT:nested.value:int} bazes from %{IPV4:nested.ipv4}",
			output:  `{"nested":{"ipv4":"192.0.1.11","name":"foo","value":5}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := processor.NewConfig()
			conf.Type = "grok"
			conf.Grok.Expressions = []string{test.pattern}
			conf.Grok.PatternDefinitions = test.definitions

			gSet, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			inMsg := message.QuickBatch([][]byte{[]byte(test.input)})
			msgs, _ := gSet.ProcessBatch(context.Background(), inMsg)
			require.Len(t, msgs, 1)

			assert.Equal(t, test.output, string(msgs[0].Get(0).AsBytes()))
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := processor.NewConfig()
			conf.Type = "grok"
			conf.Grok.Expressions = []string{test.pattern}
			conf.Grok.PatternDefinitions = test.definitions

			gSet, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			inMsg := message.QuickBatch([][]byte{[]byte(test.input)})
			msgs, _ := gSet.ProcessBatch(context.Background(), inMsg)
			require.Len(t, msgs, 1)

			assert.Equal(t, test.output, string(msgs[0].Get(0).AsBytes()))
		})
	}
}

func TestGrokFileImports(t *testing.T) {
	tmpDir := t.TempDir()

	err := os.WriteFile(filepath.Join(tmpDir, "foos"), []byte(`
FOOFLAT %{WORD:first} %{WORD:second} %{WORD:third}
FOONESTED %{INT:nested.first:int} %{WORD:nested.second} %{WORD:nested.third}
`), 0o777)
	require.NoError(t, err)

	conf := processor.NewConfig()
	conf.Type = "grok"
	conf.Grok.Expressions = []string{`%{FOONESTED}`, `%{FOOFLAT}`}
	conf.Grok.PatternPaths = []string{tmpDir}

	gSet, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	inMsg := message.QuickBatch([][]byte{[]byte(`hello foo bar`)})
	msgs, _ := gSet.ProcessBatch(context.Background(), inMsg)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"first":"hello","second":"foo","third":"bar"}`, string(msgs[0].Get(0).AsBytes()))

	inMsg = message.QuickBatch([][]byte{[]byte(`10 foo bar`)})
	msgs, _ = gSet.ProcessBatch(context.Background(), inMsg)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"nested":{"first":10,"second":"foo","third":"bar"}}`, string(msgs[0].Get(0).AsBytes()))
}
