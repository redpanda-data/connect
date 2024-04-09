package pure_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestWorkflowDeps(t *testing.T) {
	tests := []struct {
		branches      [][2]string
		inputOrdering [][]string
		ordering      [][]string
		errContains   string
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
			errContains: "failed to automatically resolve DAG, circular dependencies detected for branches: [0 1 2]",
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
			errContains: "the following branches were missing from order: [2]",
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
			errContains: "branch specified in order listed multiple times: 1",
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
			if test.inputOrdering == nil {
				test.inputOrdering = [][]string{}
			}
			confStr := fmt.Sprintf(`
workflow:
  order: %v
  branches:
`, gabs.Wrap(test.inputOrdering).String())

			for j, mappings := range test.branches {
				confStr += fmt.Sprintf(`
    %v:
      request_map: |
        %v
      processors:
        - bloblang: root = this
      result_map: |
        %v
`,
					strconv.Itoa(j),
					strings.ReplaceAll(mappings[0], "\n", "\n        "),
					strings.ReplaceAll(mappings[1], "\n", "\n        "),
				)
			}

			conf, err := testutil.ProcessorFromYAML(confStr)
			require.NoError(t, err)

			p, err := mock.NewManager().NewProcessor(conf)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				dag := p.(*pure.Workflow).Flow()
				for _, d := range dag {
					sort.Strings(d)
				}
				assert.Equal(t, test.ordering, dag)
			}
		})
	}
}

func newMockProcProvider(t *testing.T, confs map[string]processor.Config) bundle.NewManagement {
	t.Helper()

	resConf := manager.NewResourceConfig()
	for k, v := range confs {
		v.Label = k
		resConf.ResourceProcessors = append(resConf.ResourceProcessors, v)
	}
	mgr, err := manager.New(resConf)
	require.NoError(t, err)

	return mgr
}

func quickTestBranches(t testing.TB, branches ...[4]string) map[string]processor.Config {
	t.Helper()
	m := map[string]processor.Config{}
	for _, b := range branches {
		conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
branch:
  request_map: |
    %v
  processors:
    - bloblang: |
        %v
  result_map: |
    %v
`,
			strings.ReplaceAll(b[1], "\n", "\n    "),
			strings.ReplaceAll(b[2], "\n", "\n        "),
			strings.ReplaceAll(b[3], "\n", "\n    "),
		))
		require.NoError(t, err)

		m[b[0]] = conf
	}
	return m
}

func TestWorkflowMissingResources(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
workflow:
  order: [[ foo, bar, baz ]]
  branches:
    bar:
      request_map: root = this
      processors:
        - bloblang: root = this
      result_map: root = this
`)
	require.NoError(t, err)

	branchConf, err := testutil.ProcessorFromYAML(`
branch:
  request_map: root = this
  processors:
    - bloblang: root = this
  result_map: root = this
`)
	require.NoError(t, err)

	mgr := newMockProcProvider(t, map[string]processor.Config{
		"baz": branchConf,
	})

	_, err = mgr.NewProcessor(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "processor resource 'foo' was not found")
}

type mockMsg struct {
	content string
	meta    map[string]string
	err     error
}

func (m mockMsg) withErr(err error) mockMsg {
	m.err = err
	return m
}

func TestWorkflows(t *testing.T) {
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
				msg(`not even a json object`).withErr(errors.New("invalid character 'o' in literal null (expecting 'u')")),
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
				msg(`{"id":0,"name":"first"}`).withErr(errors.New("this is a pre-existing failure")),
				msg(`{"failme":true,"id":1,"name":"second"}`),
				msg(`{"failme":true,"id":2,"name":"third"}`).withErr(errors.New("this is a pre-existing failure")),
			},
			output: []mockMsg{
				msg(`{"id":0,"meta":{"workflow":{"succeeded":["0"]}},"name":"first","result":"FIRST"}`).withErr(errors.New("this is a pre-existing failure")),
				msg(
					`{"failme":true,"id":1,"meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): this is a branch error"}}},"name":"second"}`,
				),
				msg(`{"failme":true,"id":2,"meta":{"workflow":{"failed":{"0":"result mapping failed: failed assignment (line 1): this is a branch error"}}},"name":"third"}`).withErr(errors.New("this is a pre-existing failure")),
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if test.order == nil {
				test.order = [][]string{}
			}
			confStr := fmt.Sprintf(`
workflow:
  order: %v
  branches:
`, gabs.Wrap(test.order).String())

			for j, mappings := range test.branches {
				confStr += fmt.Sprintf(`
    %v:
      request_map: |
        %v
      processors:
        - bloblang: |
            %v
      result_map: |
        %v
`,
					strconv.Itoa(j),
					strings.ReplaceAll(mappings[0], "\n", "\n        "),
					strings.ReplaceAll(mappings[1], "\n", "\n            "),
					strings.ReplaceAll(mappings[2], "\n", "\n        "),
				)
			}

			conf, err := testutil.ProcessorFromYAML(confStr)
			require.NoError(t, err)

			p, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			inputMsg := message.QuickBatch(nil)
			for _, m := range test.input {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.MetaSetMut(k, v)
					}
				}
				if m.err != nil {
					part.ErrorSet(m.err)
				}
				inputMsg = append(inputMsg, part)
			}

			msgs, res := p.ProcessBatch(context.Background(), inputMsg.ShallowCopy())
			if test.err != "" {
				require.Error(t, res)
				require.EqualError(t, res, test.err)
			} else {
				require.Len(t, msgs, 1)
				assert.Equal(t, len(test.output), msgs[0].Len())
				for i, out := range test.output {
					comparePart := mockMsg{
						content: string(msgs[0].Get(i).AsBytes()),
						meta:    map[string]string{},
					}

					_ = msgs[0].Get(i).MetaIterStr(func(k, v string) error {
						comparePart.meta[k] = v
						return nil
					})

					if out.err != nil {
						assert.EqualError(t, msgs[0].Get(i).ErrorGet(), out.err.Error())
					} else {
						assert.NoError(t, msgs[0].Get(i).ErrorGet())
					}
					msgs[0].Get(i).ErrorSet(nil)
					out.err = nil

					assert.Equal(t, out, comparePart, "part: %v", i)
				}
			}

			// Ensure nothing changed
			for i, m := range test.input {
				assert.Equal(t, m.content, string(inputMsg.Get(i).AsBytes()))
			}

			ctx, done := context.WithTimeout(context.Background(), time.Second*30)
			defer done()
			assert.NoError(t, p.Close(ctx))
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
			var branchNames []string
			for _, b := range test.branches {
				branchNames = append(branchNames, b[0])
			}

			mgr := newMockProcProvider(t, quickTestBranches(t, test.branches...))

			conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
workflow:
  branch_resources: %v
`, gabs.Wrap(branchNames).String()))
			require.NoError(t, err)

			p, err := mgr.NewProcessor(conf)
			require.NoError(t, err)

			var parts [][]byte
			for _, input := range test.input {
				parts = append(parts, []byte(input))
			}

			msgs, res := p.ProcessBatch(context.Background(), message.QuickBatch(parts))
			if test.err != "" {
				require.Error(t, res)
				require.EqualError(t, res, test.err)
			} else {
				require.Len(t, msgs, 1)
				var output []string
				for _, b := range message.GetAllBytes(msgs[0]) {
					output = append(output, string(b))
				}
				assert.Equal(t, test.output, output)
			}

			ctx, done := context.WithTimeout(context.Background(), time.Second*30)
			defer done()
			assert.NoError(t, p.Close(ctx))
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

	var branchNames []string
	for _, b := range branches {
		branchNames = append(branchNames, b[0])
	}

	conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
workflow:
  branch_resources: %v
`, gabs.Wrap(branchNames).String()))
	require.NoError(t, err)

	for loops := 0; loops < 10; loops++ {
		mgr := newMockProcProvider(t, quickTestBranches(t, branches...))
		p, err := mgr.NewProcessor(conf)
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

					msgs, res := p.ProcessBatch(context.Background(), message.QuickBatch(parts))
					require.NoError(t, res)
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

		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		assert.NoError(t, p.Close(ctx))
		done()
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
			if test.order == nil {
				test.order = [][]string{}
			}
			conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
workflow:
  order: %v
`, gabs.Wrap(test.order).String()))
			require.NoError(t, err)

			mgr := newMockProcProvider(t, quickTestBranches(t, test.branches...))
			p, err := mgr.NewProcessor(conf)
			require.NoError(t, err)

			var parts [][]byte
			for _, input := range test.input {
				parts = append(parts, []byte(input))
			}

			msgs, res := p.ProcessBatch(context.Background(), message.QuickBatch(parts))
			if test.err != "" {
				require.Error(t, res)
				require.EqualError(t, res, test.err)
			} else {
				require.Len(t, msgs, 1)
				var output []string
				for _, b := range message.GetAllBytes(msgs[0]) {
					output = append(output, string(b))
				}
				assert.Equal(t, test.output, output)
			}

			ctx, done := context.WithTimeout(context.Background(), time.Second*30)
			defer done()
			assert.NoError(t, p.Close(ctx))
		})
	}
}

func TestWorkflowUnwrapResourceBranches(t *testing.T) {
	strmBuilder := service.NewStreamBuilder()

	require.NoError(t, strmBuilder.AddResourcesYAML(`
processor_resources:
  - label: fooproc
    branch:
      request_map: 'root = this.id'
      processors:
        - label: innerproc
          mapping: 'root.id = content().uppercase().string()'
      result_map: 'root.id = this.id'
`))

	require.NoError(t, strmBuilder.AddProcessorYAML(`
label: barproc
workflow:
  branch_resources: [ fooproc ]
`))

	inFunc, err := strmBuilder.AddProducerFunc()
	require.NoError(t, err)

	var outValue string
	require.NoError(t, strmBuilder.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		outBytes, err := m.AsBytes()
		require.NoError(t, err)
		outValue = string(outBytes)
		return nil
	}))

	strm, tracer, err := strmBuilder.BuildTraced()
	require.NoError(t, err)

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	go func() {
		assert.NoError(t, strm.Run(tCtx))
	}()
	require.NoError(t, inFunc(tCtx, service.NewMessage([]byte(`{"id":"hello world","content":"waddup"}`))))
	require.NoError(t, strm.Stop(tCtx))

	assert.Equal(t, `{"content":"waddup","id":"HELLO WORLD","meta":{"workflow":{"succeeded":["fooproc"]}}}`, outValue)
	assert.Equal(t, map[string][]service.TracingEvent{
		"barproc": {
			{Type: "CONSUME", Content: "{\"id\":\"hello world\",\"content\":\"waddup\"}", Meta: map[string]interface{}{}},
			{Type: "PRODUCE", Content: "{\"content\":\"waddup\",\"id\":\"HELLO WORLD\",\"meta\":{\"workflow\":{\"succeeded\":[\"fooproc\"]}}}", Meta: map[string]interface{}{}},
		},
		"fooproc": {},
		"innerproc": {
			{Type: "CONSUME", Content: "hello world", Meta: map[string]interface{}{}},
			{Type: "PRODUCE", Content: "{\"id\":\"HELLO WORLD\"}", Meta: map[string]interface{}{}},
		},
	}, tracer.ProcessorEvents())
}
