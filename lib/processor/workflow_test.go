package processor

import (
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowDeps(t *testing.T) {
	tests := []struct {
		branches      [][2]string
		inputOrdering [][]string
		ordering      [][]string
		err           string
	}{
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.bar",
					"root.baz = this",
				},
				{
					"root = this.baz",
					"root.buz = this",
				},
			},
			ordering: [][]string{
				{"0"}, {"1"}, {"2"},
			},
		},
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.bar",
					"root.baz = this",
				},
				{
					"root = this.baz",
					"root.buz = this",
				},
			},
			inputOrdering: [][]string{
				{"1", "2"}, {"0"},
			},
			ordering: [][]string{
				{"1", "2"}, {"0"},
			},
		},
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.bar",
					"root.baz = this",
				},
				{
					"root = this.baz",
					"root.buz = this",
				},
			},
			ordering: [][]string{
				{"0"}, {"1"}, {"2"},
			},
		},
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.foo",
					"root.baz = this",
				},
				{
					"root = this.baz",
					"root.foo = this",
				},
			},
			err: "failed to automatically resolve DAG, circular dependencies detected for branches: [0 1 2]",
		},
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.bar",
					"root.baz = this",
				},
				{
					"root = this.baz",
					"root.buz = this",
				},
			},
			inputOrdering: [][]string{
				{"1"}, {"0"},
			},
			err: "the following branches were missing from order: [2]",
		},
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.bar",
					"root.baz = this",
				},
				{
					"root = this.baz",
					"root.buz = this",
				},
			},
			inputOrdering: [][]string{
				{"1"}, {"0", "2"}, {"1"},
			},
			err: "branch specified in order listed multiple times: 1",
		},
		{
			branches: [][2]string{
				{
					"root = this.foo",
					"root.bar = this",
				},
				{
					"root = this.foo",
					"root.baz = this",
				},
				{
					`root.bar = this.bar
					root.baz = this.baz`,
					"root.buz = this",
				},
			},
			ordering: [][]string{
				{"0", "1"}, {"2"},
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			conf := NewConfig()
			conf.Workflow.Order = test.inputOrdering
			for j, mappings := range test.branches {
				branchConf := NewBranchConfig()
				branchConf.RequestMap = mappings[0]
				branchConf.ResultMap = mappings[1]
				dudProc := NewConfig()
				dudProc.Type = TypeBloblang
				dudProc.Bloblang = BloblangConfig("root = this")
				branchConf.Processors = append(branchConf.Processors, dudProc)
				conf.Workflow.Branches[strconv.Itoa(j)] = branchConf
			}

			p, err := NewWorkflow(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
			if len(test.err) > 0 {
				assert.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)

				dag := p.(*Workflow).children.dag
				for _, d := range dag {
					sort.Strings(d)
				}
				assert.Equal(t, test.ordering, dag)
			}
		})
	}
}

func newMockProcProvider(t *testing.T, confs map[string]Config) types.Manager {
	t.Helper()

	procs := map[string]Type{}

	for k, v := range confs {
		var err error
		procs[k], err = New(v, nil, log.Noop(), metrics.Noop())
		require.NoError(t, err)
	}

	return &fakeProcMgr{
		procs: procs,
	}
}

func quickTestBranches(branches ...[4]string) map[string]Config {
	m := map[string]Config{}
	for _, b := range branches {
		blobConf := NewConfig()
		blobConf.Type = TypeBloblang
		blobConf.Bloblang = BloblangConfig(b[2])

		conf := NewConfig()
		conf.Type = TypeBranch
		conf.Branch.RequestMap = b[1]
		conf.Branch.Processors = append(conf.Branch.Processors, blobConf)
		conf.Branch.ResultMap = b[3]

		m[b[0]] = conf
	}
	return m
}

func TestWorkflowMissingResources(t *testing.T) {
	conf := NewConfig()
	conf.Workflow.Order = [][]string{
		{"foo", "bar", "baz"},
	}

	branchConf := NewConfig()
	branchConf.Branch.RequestMap = "root = this"
	branchConf.Branch.ResultMap = "root = this"

	blobConf := NewConfig()
	blobConf.Type = TypeBloblang
	blobConf.Bloblang = "root = this"

	branchConf.Branch.Processors = append(branchConf.Branch.Processors, blobConf)

	conf.Workflow.Branches["bar"] = branchConf.Branch

	mgr := newMockProcProvider(t, map[string]Config{
		"baz": branchConf,
	})

	_, err := NewWorkflow(conf, mgr, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "processor resource 'foo' was not found")
}

func TestWorkflows(t *testing.T) {
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

	// To make configs simpler they break branches down into three mappings, the
	// request map, a bloblang processor, and a result map.
	tests := []struct {
		branches [][3]string
		order    [][]string
		input    []mockMsg
		output   []mockMsg
		err      string
	}{
		{
			branches: [][3]string{
				{
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
			},
			input: []mockMsg{
				msg(`{}`),
				msg(`{"foo":"not a number"}`),
				msg(`{"foo":"5"}`),
			},
			output: []mockMsg{
				msg(`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null"}}}}`),
				msg(`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax"}}}}`),
				msg(`{"bar":5,"foo":"5","meta":{"workflow":{"succeeded":["0"]}}}`),
			},
		},
		{
			branches: [][3]string{
				{
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
				{
					"root.bar = this.bar.not_null()",
					"root = this",
					"root.baz = this.bar.number() + 5",
				},
				{
					"root.baz = this.baz.not_null()",
					"root = this",
					"root.buz = this.baz.number() + 2",
				},
			},
			input: []mockMsg{
				msg(`{}`),
				msg(`{"foo":"not a number"}`),
				msg(`{"foo":"5"}`),
			},
			output: []mockMsg{
				msg(`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`),
				msg(`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`),
				msg(`{"bar":5,"baz":10,"buz":12,"foo":"5","meta":{"workflow":{"succeeded":["0","1","2"]}}}`),
			},
		},
		{
			branches: [][3]string{
				{
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
				{
					"root.bar = this.bar.not_null()",
					"root = this",
					"root.baz = this.bar.number() + 5",
				},
				{
					"root.baz = this.baz.not_null()",
					"root = this",
					"root.buz = this.baz.number() + 2",
				},
			},
			input: []mockMsg{
				msg(`{"meta":{"workflow":{"apply":["2"]}},"baz":2}`),
				msg(`{"meta":{"workflow":{"skipped":["0"]}},"bar":3}`),
				msg(`{"meta":{"workflow":{"succeeded":["1"]}},"baz":9}`),
			},
			output: []mockMsg{
				msg(`{"baz":2,"buz":4,"meta":{"workflow":{"previous":{"apply":["2"]},"skipped":["0","1"],"succeeded":["2"]}}}`),
				msg(`{"bar":3,"baz":8,"buz":10,"meta":{"workflow":{"previous":{"skipped":["0"]},"skipped":["0"],"succeeded":["1","2"]}}}`),
				msg(`{"baz":9,"buz":11,"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null"},"previous":{"succeeded":["1"]},"skipped":["1"],"succeeded":["2"]}}}`),
			},
		},
		{
			branches: [][3]string{
				{
					"root = this.foo.not_null()",
					"root = this",
					"root.bar = this.number() + 2",
				},
				{
					"root = this.foo.not_null()",
					"root = this",
					"root.baz = this.number() + 3",
				},
				{
					`root.bar = this.bar.not_null()
					root.baz = this.baz.not_null()`,
					"root = this",
					"root.buz = this.bar + this.baz",
				},
			},
			input: []mockMsg{
				msg(`{"foo":2}`),
				msg(`{}`),
				msg(`not even a json object`),
			},
			output: []mockMsg{
				msg(`{"bar":4,"baz":5,"buz":9,"foo":2,"meta":{"workflow":{"succeeded":["0","1","2"]}}}`),
				msg(`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null"}}}}`),
				msg(
					`not even a json object`,
					FailFlagKey,
					"invalid character 'o' in literal null (expecting 'u')",
				),
			},
		},
		{
			branches: [][3]string{
				{
					`root = this`,
					`root = this
					 root.name_upper = this.name.uppercase()`,
					`root.result = if this.failme.bool(false) {
						throw("this is a branch error")
					} else {
						this.name_upper
					}`,
				},
			},
			input: []mockMsg{
				msg(
					`{"id":0,"name":"first"}`,
					FailFlagKey, "this is a pre-existing failure",
				),
				msg(`{"failme":true,"id":1,"name":"second"}`),
				msg(
					`{"failme":true,"id":2,"name":"third"}`,
					FailFlagKey, "this is a pre-existing failure",
				),
			},
			output: []mockMsg{
				msg(
					`{"id":0,"meta":{"workflow":{"succeeded":["0"]}},"name":"first","result":"FIRST"}`,
					FailFlagKey, "this is a pre-existing failure",
				),
				msg(
					`{"failme":true,"id":1,"meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): this is a branch error"}}},"name":"second"}`,
				),
				msg(
					`{"failme":true,"id":2,"meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): this is a branch error"}}},"name":"third"}`,
					FailFlagKey, "this is a pre-existing failure",
				),
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			conf := NewConfig()
			conf.Workflow.Order = test.order
			for j, mappings := range test.branches {
				branchConf := NewBranchConfig()
				branchConf.RequestMap = mappings[0]
				branchConf.ResultMap = mappings[2]
				proc := NewConfig()
				proc.Type = TypeBloblang
				proc.Bloblang = BloblangConfig(mappings[1])
				branchConf.Processors = append(branchConf.Processors, proc)
				conf.Workflow.Branches[strconv.Itoa(j)] = branchConf
			}

			p, err := NewWorkflow(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
			require.NoError(t, err)

			inputMsg := message.QuickBatch(nil)
			for _, m := range test.input {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.MetaSet(k, v)
					}
				}
				inputMsg.Append(part)
			}

			msgs, res := p.ProcessMessage(inputMsg)
			if len(test.err) > 0 {
				require.NotNil(t, res)
				require.EqualError(t, res.Error(), test.err)
			} else {
				require.Len(t, msgs, 1)
				assert.Equal(t, len(test.output), msgs[0].Len())
				for i, out := range test.output {
					comparePart := mockMsg{
						content: string(msgs[0].Get(i).Get()),
						meta:    map[string]string{},
					}

					_ = msgs[0].Get(i).MetaIter(func(k, v string) error {
						comparePart.meta[k] = v
						return nil
					})

					assert.Equal(t, out, comparePart, "part: %v", i)
				}
			}

			// Ensure nothing changed
			for i, m := range test.input {
				assert.Equal(t, m.content, string(inputMsg.Get(i).Get()))
			}

			p.CloseAsync()
			assert.NoError(t, p.WaitForClose(time.Second))
		})
	}
}

func TestWorkflowsWithResources(t *testing.T) {
	// To make configs simpler they break branches down into three mappings, the
	// request map, a bloblang processor, and a result map.
	tests := []struct {
		branches [][4]string
		input    []string
		output   []string
		err      string
	}{
		{
			branches: [][4]string{
				{
					"0",
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
			},
			input: []string{
				`{}`,
				`{"foo":"not a number"}`,
				`{"foo":"5"}`,
			},
			output: []string{
				`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null"}}}}`,
				`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax"}}}}`,
				`{"bar":5,"foo":"5","meta":{"workflow":{"succeeded":["0"]}}}`,
			},
		},
		{
			branches: [][4]string{
				{
					"0",
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
				{
					"1",
					"root.bar = this.bar.not_null()",
					"root = this",
					"root.baz = this.bar.number() + 5",
				},
				{
					"2",
					"root.baz = this.baz.not_null()",
					"root = this",
					"root.buz = this.baz.number() + 2",
				},
			},
			input: []string{
				`{}`,
				`{"foo":"not a number"}`,
				`{"foo":"5"}`,
			},
			output: []string{
				`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`,
				`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`,
				`{"bar":5,"baz":10,"buz":12,"foo":"5","meta":{"workflow":{"succeeded":["0","1","2"]}}}`,
			},
		},
		{
			branches: [][4]string{
				{
					"0",
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
				{
					"1",
					"root.bar = this.bar.not_null()",
					"root = this",
					"root.baz = this.bar.number() + 5",
				},
				{
					"2",
					"root.baz = this.baz.not_null()",
					"root = this",
					"root.buz = this.baz.number() + 2",
				},
			},
			input: []string{
				`{"meta":{"workflow":{"apply":["2"]}},"baz":2}`,
				`{"meta":{"workflow":{"skipped":["0"]}},"bar":3}`,
				`{"meta":{"workflow":{"succeeded":["1"]}},"baz":9}`,
			},
			output: []string{
				`{"baz":2,"buz":4,"meta":{"workflow":{"previous":{"apply":["2"]},"skipped":["0","1"],"succeeded":["2"]}}}`,
				`{"bar":3,"baz":8,"buz":10,"meta":{"workflow":{"previous":{"skipped":["0"]},"skipped":["0"],"succeeded":["1","2"]}}}`,
				`{"baz":9,"buz":11,"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null"},"previous":{"succeeded":["1"]},"skipped":["1"],"succeeded":["2"]}}}`,
			},
		},
		{
			branches: [][4]string{
				{
					"0",
					"root = this.foo.not_null()",
					"root = this",
					"root.bar = this.number() + 2",
				},
				{
					"1",
					"root = this.foo.not_null()",
					"root = this",
					"root.baz = this.number() + 3",
				},
				{
					"2",
					`root.bar = this.bar.not_null()
					root.baz = this.baz.not_null()`,
					"root = this",
					"root.buz = this.bar + this.baz",
				},
			},
			input: []string{
				`{"foo":2}`,
				`{}`,
				`not even a json object`,
			},
			output: []string{
				`{"bar":4,"baz":5,"buz":9,"foo":2,"meta":{"workflow":{"succeeded":["0","1","2"]}}}`,
				`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null"}}}}`,
				`not even a json object`,
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			conf := NewConfig()
			conf.Workflow.BranchResources = []string{}
			for _, b := range test.branches {
				conf.Workflow.BranchResources = append(conf.Workflow.BranchResources, b[0])
			}

			mgr := newMockProcProvider(t, quickTestBranches(test.branches...))
			p, err := NewWorkflow(conf, mgr, log.Noop(), metrics.Noop())
			require.NoError(t, err)

			var parts [][]byte
			for _, input := range test.input {
				parts = append(parts, []byte(input))
			}

			msgs, res := p.ProcessMessage(message.QuickBatch(parts))
			if len(test.err) > 0 {
				require.NotNil(t, res)
				require.EqualError(t, res.Error(), test.err)
			} else {
				require.Len(t, msgs, 1)
				var output []string
				for _, b := range message.GetAllBytes(msgs[0]) {
					output = append(output, string(b))
				}
				assert.Equal(t, test.output, output)
			}

			p.CloseAsync()
			assert.NoError(t, p.WaitForClose(time.Second))
		})
	}
}

func TestWorkflowsParallel(t *testing.T) {
	branches := [][4]string{
		{
			"0",
			"root.foo = this.foo.not_null()",
			"root = this",
			"root.bar = this.foo.number()",
		},
		{
			"1",
			"root.bar = this.bar.not_null()",
			"root = this",
			"root.baz = this.bar.number() + 5",
		},
		{
			"2",
			"root.baz = this.baz.not_null()",
			"root = this",
			"root.buz = this.baz.number() + 2",
		},
	}
	input := []string{
		`{}`,
		`{"foo":"not a number"}`,
		`{"foo":"5"}`,
	}
	output := []string{
		`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`,
		`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`,
		`{"bar":5,"baz":10,"buz":12,"foo":"5","meta":{"workflow":{"succeeded":["0","1","2"]}}}`,
	}

	conf := NewConfig()
	conf.Workflow.BranchResources = []string{}
	for _, b := range branches {
		conf.Workflow.BranchResources = append(conf.Workflow.BranchResources, b[0])
	}

	for loops := 0; loops < 10; loops++ {
		mgr := newMockProcProvider(t, quickTestBranches(branches...))
		p, err := NewWorkflow(conf, mgr, log.Noop(), metrics.Noop())
		require.NoError(t, err)

		startChan := make(chan struct{})
		wg := sync.WaitGroup{}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startChan

				for j := 0; j < 100; j++ {
					var parts [][]byte
					for _, input := range input {
						parts = append(parts, []byte(input))
					}

					msgs, res := p.ProcessMessage(message.QuickBatch(parts))
					require.Nil(t, res)
					require.Len(t, msgs, 1)
					var actual []string
					for _, b := range message.GetAllBytes(msgs[0]) {
						actual = append(actual, string(b))
					}
					assert.Equal(t, output, actual)
				}
			}()
		}

		close(startChan)
		wg.Wait()

		p.CloseAsync()
		assert.NoError(t, p.WaitForClose(time.Second))
	}
}

func TestWorkflowsWithOrderResources(t *testing.T) {
	// To make configs simpler they break branches down into three mappings, the
	// request map, a bloblang processor, and a result map.
	tests := []struct {
		branches [][4]string
		order    [][]string
		input    []string
		output   []string
		err      string
	}{
		{
			branches: [][4]string{
				{
					"0",
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
			},
			order: [][]string{
				{"0"},
			},
			input: []string{
				`{}`,
				`{"foo":"not a number"}`,
				`{"foo":"5"}`,
			},
			output: []string{
				`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null"}}}}`,
				`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax"}}}}`,
				`{"bar":5,"foo":"5","meta":{"workflow":{"succeeded":["0"]}}}`,
			},
		},
		{
			branches: [][4]string{
				{
					"0",
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
				{
					"1",
					"root.bar = this.bar.not_null()",
					"root = this",
					"root.baz = this.bar.number() + 5",
				},
				{
					"2",
					"root.baz = this.baz.not_null()",
					"root = this",
					"root.buz = this.baz.number() + 2",
				},
			},
			order: [][]string{
				{"0"},
				{"1"},
				{"2"},
			},
			input: []string{
				`{}`,
				`{"foo":"not a number"}`,
				`{"foo":"5"}`,
			},
			output: []string{
				`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`,
				`{"foo":"not a number","meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: strconv.ParseFloat: parsing \"not a number\": invalid syntax","1":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.baz`" + `: value is null"}}}}`,
				`{"bar":5,"baz":10,"buz":12,"foo":"5","meta":{"workflow":{"succeeded":["0","1","2"]}}}`,
			},
		},
		{
			branches: [][4]string{
				{
					"0",
					"root.foo = this.foo.not_null()",
					"root = this",
					"root.bar = this.foo.number()",
				},
				{
					"1",
					"root.bar = this.bar.not_null()",
					"root = this",
					"root.baz = this.bar.number() + 5",
				},
				{
					"2",
					"root.baz = this.baz.not_null()",
					"root = this",
					"root.buz = this.baz.number() + 2",
				},
			},
			order: [][]string{
				{"0"},
				{"1"},
				{"2"},
			},
			input: []string{
				`{"meta":{"workflow":{"apply":["2"]}},"baz":2}`,
				`{"meta":{"workflow":{"skipped":["0"]}},"bar":3}`,
				`{"meta":{"workflow":{"succeeded":["1"]}},"baz":9}`,
			},
			output: []string{
				`{"baz":2,"buz":4,"meta":{"workflow":{"previous":{"apply":["2"]},"skipped":["0","1"],"succeeded":["2"]}}}`,
				`{"bar":3,"baz":8,"buz":10,"meta":{"workflow":{"previous":{"skipped":["0"]},"skipped":["0"],"succeeded":["1","2"]}}}`,
				`{"baz":9,"buz":11,"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null"},"previous":{"succeeded":["1"]},"skipped":["1"],"succeeded":["2"]}}}`,
			},
		},
		{
			branches: [][4]string{
				{
					"0",
					"root = this.foo.not_null()",
					"root = this",
					"root.bar = this.number() + 2",
				},
				{
					"1",
					"root = this.foo.not_null()",
					"root = this",
					"root.baz = this.number() + 3",
				},
				{
					"2",
					`root.bar = this.bar.not_null()
					root.baz = this.baz.not_null()`,
					"root = this",
					"root.buz = this.bar + this.baz",
				},
			},
			order: [][]string{
				{"0", "1"},
				{"2"},
			},
			input: []string{
				`{"foo":2}`,
				`{}`,
				`not even a json object`,
			},
			output: []string{
				`{"bar":4,"baz":5,"buz":9,"foo":2,"meta":{"workflow":{"succeeded":["0","1","2"]}}}`,
				`{"meta":{"workflow":{"failed":{"0":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","1":"request mapping failed: failed assignment (line 1): field ` + "`this.foo`" + `: value is null","2":"request mapping failed: failed assignment (line 1): field ` + "`this.bar`" + `: value is null"}}}}`,
				`not even a json object`,
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			conf := NewConfig()
			conf.Workflow.Order = test.order

			mgr := newMockProcProvider(t, quickTestBranches(test.branches...))
			p, err := NewWorkflow(conf, mgr, log.Noop(), metrics.Noop())
			require.NoError(t, err)

			var parts [][]byte
			for _, input := range test.input {
				parts = append(parts, []byte(input))
			}

			msgs, res := p.ProcessMessage(message.QuickBatch(parts))
			if len(test.err) > 0 {
				require.NotNil(t, res)
				require.EqualError(t, res.Error(), test.err)
			} else {
				require.Len(t, msgs, 1)
				var output []string
				for _, b := range message.GetAllBytes(msgs[0]) {
					output = append(output, string(b))
				}
				assert.Equal(t, test.output, output)
			}

			p.CloseAsync()
			assert.NoError(t, p.WaitForClose(time.Second))
		})
	}
}
