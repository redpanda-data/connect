package processor

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBranchBasic(t *testing.T) {
	type mockMsg struct {
		content string
		meta    map[string]string
	}
	msg := func(content string, meta ...string) mockMsg {
		t.Helper()
		m := mockMsg{
			content: content,
			meta:    map[string]string{},
		}
		for i, v := range meta {
			if i%2 == 1 {
				m.meta[meta[i-1]] = v
			}
		}
		return m
	}

	tests := map[string]struct {
		requestMap   string
		processorMap string
		resultMap    string
		input        []mockMsg
		output       []mockMsg
	}{
		"empty request mapping": {
			requestMap:   "",
			processorMap: "root.nested = this",
			resultMap:    "root.result = this.nested",
			input: []mockMsg{
				msg(`{"value":"foobar"}`),
			},
			output: []mockMsg{
				msg(`{"result":{"value":"foobar"},"value":"foobar"}`),
			},
		},
		"empty result mapping": {
			requestMap:   "root.nested = this",
			processorMap: "root = this",
			resultMap:    "",
			input: []mockMsg{
				msg(`{"value":"foobar"}`),
			},
			output: []mockMsg{
				msg(`{"value":"foobar"}`),
			},
		},
		"copy metadata over only": {
			requestMap:   `meta foo = meta("foo")`,
			processorMap: `meta foo = meta("foo") + " and this"`,
			resultMap:    `meta new_foo = meta("foo")`,
			input: []mockMsg{
				msg(
					`{"value":"foobar"}`,
					"foo", "bar",
				),
			},
			output: []mockMsg{
				msg(
					`{"value":"foobar"}`,
					"foo", "bar",
					"new_foo", "bar and this",
				),
			},
		},
		"filtered and failed mappings": {
			requestMap: `root = match {
				this.id == 0 => throw("i dont like zero"),
				this.id == 3 => deleted(),
				_ => {"name":this.name,"id":this.id}
			}`,
			processorMap: `root = this
			root.name_upper = this.name.uppercase()`,
			resultMap: `root.result = match {
				this.id == 2 => throw("i dont like two either"),
				_ => this.name_upper
			}`,
			input: []mockMsg{
				msg(`{"id":0,"name":"first"}`),
				msg(`{"id":1,"name":"second"}`),
				msg(`{"id":2,"name":"third"}`),
				msg(`{"id":3,"name":"fourth"}`),
				msg(`{"id":4,"name":"fifth"}`),
			},
			output: []mockMsg{
				msg(
					`{"id":0,"name":"first"}`,
					FailFlagKey,
					"request failed: failed to execute mapping query at line 1: i dont like zero",
				),
				msg(`{"id":1,"name":"second","result":"SECOND"}`),
				msg(
					`{"id":2,"name":"third"}`,
					FailFlagKey,
					"response failed: failed to execute mapping query at line 1: i dont like two either",
				),
				msg(`{"id":3,"name":"fourth"}`),
				msg(`{"id":4,"name":"fifth","result":"FIFTH"}`),
			},
		},
		"filter all requests": {
			requestMap:   `root = deleted()`,
			processorMap: `root = this`,
			resultMap:    `root.result = this`,
			input: []mockMsg{
				msg(`{"id":0,"name":"first"}`),
				msg(`{"id":1,"name":"second"}`),
				msg(`{"id":2,"name":"third"}`),
				msg(`{"id":3,"name":"fourth"}`),
				msg(`{"id":4,"name":"fifth"}`),
			},
			output: []mockMsg{
				msg(`{"id":0,"name":"first"}`),
				msg(`{"id":1,"name":"second"}`),
				msg(`{"id":2,"name":"third"}`),
				msg(`{"id":3,"name":"fourth"}`),
				msg(`{"id":4,"name":"fifth"}`),
			},
		},
		"filter during processing": {
			requestMap:   `root = if this.id == 3 { throw("foo") } else { this }`,
			processorMap: `root = deleted()`,
			resultMap:    `root.result = this`,
			input: []mockMsg{
				msg(`{"id":0,"name":"first"}`),
				msg(`{"id":1,"name":"second"}`),
				msg(`{"id":2,"name":"third"}`),
				msg(`{"id":3,"name":"fourth"}`),
				msg(`{"id":4,"name":"fifth"}`),
			},
			output: []mockMsg{
				msg(
					`{"id":0,"name":"first"}`,
					FailFlagKey,
					"child processors resulted in zero messages",
				),
				msg(
					`{"id":1,"name":"second"}`,
					FailFlagKey,
					"child processors resulted in zero messages",
				),
				msg(
					`{"id":2,"name":"third"}`,
					FailFlagKey,
					"child processors resulted in zero messages",
				),
				msg(
					`{"id":3,"name":"fourth"}`,
					FailFlagKey,
					"request failed: failed to execute mapping query at line 1: foo",
				),
				msg(
					`{"id":4,"name":"fifth"}`,
					FailFlagKey,
					"child processors resulted in zero messages",
				),
			},
		},
		"filter some during processing": {
			requestMap:   `root = if this.id == 3 { throw("foo") } else { this }`,
			processorMap: `root = if this.id == 2 { deleted() }`,
			resultMap:    `root.result = this`,
			input: []mockMsg{
				msg(`{"id":0,"name":"first"}`),
				msg(`{"id":1,"name":"second"}`),
				msg(`{"id":2,"name":"third"}`),
				msg(`{"id":3,"name":"fourth"}`),
				msg(`{"id":4,"name":"fifth"}`),
			},
			output: []mockMsg{
				msg(
					`{"id":0,"name":"first"}`,
					FailFlagKey,
					"message count returned from branch does not match request: 4 != 5",
				),
				msg(
					`{"id":1,"name":"second"}`,
					FailFlagKey,
					"message count returned from branch does not match request: 4 != 5",
				),
				msg(
					`{"id":2,"name":"third"}`,
					FailFlagKey,
					"message count returned from branch does not match request: 4 != 5",
				),
				msg(
					`{"id":3,"name":"fourth"}`,
					FailFlagKey,
					"request failed: failed to execute mapping query at line 1: foo",
				),
				msg(
					`{"id":4,"name":"fifth"}`,
					FailFlagKey,
					"message count returned from branch does not match request: 4 != 5",
				),
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			procConf := NewConfig()
			procConf.Type = TypeBloblang
			procConf.Bloblang = BloblangConfig(test.processorMap)

			conf := NewConfig()
			conf.Type = TypeBranch
			conf.Branch.RequestMap = test.requestMap
			conf.Branch.Processors = append(conf.Branch.Processors, procConf)
			conf.Branch.ResultMap = test.resultMap

			proc, err := NewBranch(conf, nil, log.Noop(), metrics.Noop())
			require.NoError(t, err)

			msg := message.New(nil)
			for _, m := range test.input {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.Metadata().Set(k, v)
					}
				}
				msg.Append(part)
			}

			for i := 0; i < 10; i++ {
				outMsgs, res := proc.ProcessMessage(msg)

				require.Nil(t, res)
				require.Len(t, outMsgs, 1)

				assert.Equal(t, len(test.output), outMsgs[0].Len(), i)
				for i, out := range test.output {
					comparePart := mockMsg{
						content: string(outMsgs[0].Get(i).Get()),
						meta:    map[string]string{},
					}

					outMsgs[0].Get(i).Metadata().Iter(func(k, v string) error {
						comparePart.meta[k] = v
						return nil
					})

					assert.Equal(t, out, comparePart, i)
				}
			}

			// Ensure nothing changed
			for i, m := range test.input {
				doc, err := msg.Get(i).JSON()
				if err == nil {
					msg.Get(i).SetJSON(doc)
				}
				assert.Equal(t, m.content, string(msg.Get(i).Get()))
			}

			proc.CloseAsync()
			assert.NoError(t, proc.WaitForClose(time.Second))
		})
	}
}
