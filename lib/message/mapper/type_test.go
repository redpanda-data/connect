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

package mapper

import (
	"reflect"
	"sync"
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/types"
)

func TestTypeDeps(t *testing.T) {
	expDeps := []string{
		"",
		"dep.1",
		"dep.2",
		"dep.3",
		"dep.4",
		"dep.5",
	}
	expProvs := []string{
		"prov.1",
		"prov.2",
		"prov.3",
		"prov.4",
	}

	m, err := New(OptSetReqMap(map[string]string{
		"1": "dep.1",
		"2": "",
		"3": ".",
		"4": "dep.2",
		"5": "dep.3",
	}), OptSetOptReqMap(map[string]string{
		"6": "dep.5",
		"7": "",
		"8": "dep.4",
	}), OptSetResMap(map[string]string{
		"prov.1": "1",
		"prov.2": "2",
	}), OptSetOptResMap(map[string]string{
		"prov.3": "1",
		"prov.4": "2",
	}))
	if err != nil {
		t.Fatal(err)
	}

	if act := m.TargetsUsed(); !reflect.DeepEqual(act, expDeps) {
		t.Errorf("Wrong used targets returned: %s != %s", act, expDeps)
	}
	if act := m.TargetsProvided(); !reflect.DeepEqual(act, expProvs) {
		t.Errorf("Wrong provided targets returned: %s != %s", act, expProvs)
	}
}

func TestTypeConditions(t *testing.T) {
	cConf := condition.NewConfig()
	cConf.Type = "text"
	cConf.Text.Arg = "foo"
	cConf.Text.Operator = "contains"

	cond, err := condition.New(cConf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	e, err := New(OptSetConditions([]types.Condition{cond}))
	if err != nil {
		t.Fatal(err)
	}

	if !e.test(message.New([][]byte{[]byte("foo bar")})) {
		t.Error("Expected pass")
	}
	if e.test(message.New([][]byte{[]byte("bar baz")})) {
		t.Error("Expected fail")
	}
}

func TestTypeMapValidation(t *testing.T) {
	type testCase struct {
		name   string
		resMap map[string]string
		reqMap map[string]string
		passes bool
	}

	tests := []testCase{
		{
			name: "both okay",
			resMap: map[string]string{
				"foo.bar": "baz",
				"foo.baz": "bar",
			},
			reqMap: map[string]string{
				"foo.bar": "baz",
				"foo.baz": "bar",
			},
			passes: true,
		},
		{
			name: "both okay 2",
			resMap: map[string]string{
				"foo.bar":  "baz",
				"foo.bar2": "bar",
			},
			reqMap: map[string]string{
				"foo.bar":  "baz",
				"foo.bar2": "bar",
			},
			passes: true,
		},
		{
			name: "both okay 3",
			resMap: map[string]string{
				"foo.bar.baz":  "baz",
				"foo.bar2.baz": "bar",
			},
			reqMap: map[string]string{
				"foo.bar.baz":  "baz",
				"foo.bar2.baz": "bar",
			},
			passes: true,
		},
		{
			name: "res root then override map",
			resMap: map[string]string{
				"":        "baz",
				"foo.baz": "bar",
			},
			reqMap: map[string]string{},
			passes: true,
		},
		{
			name: "res target then override map",
			resMap: map[string]string{
				"foo.bar":     "baz",
				"foo.bar.baz": "bar",
			},
			reqMap: map[string]string{},
			passes: true,
		},
		{
			name: "good res map",
			resMap: map[string]string{
				"bar":         "baz",
				"foo.baz.bar": "baz",
				"foo.bar.baz": "bar",
			},
			reqMap: map[string]string{},
			passes: true,
		},
		{
			name:   "req root then override map",
			resMap: map[string]string{},
			reqMap: map[string]string{
				"":        "baz",
				"foo.baz": "bar",
			},
			passes: true,
		},
		{
			name: "res root collision",
			resMap: map[string]string{
				"":  "baz",
				".": "bar",
			},
			reqMap: map[string]string{},
			passes: false,
		},
		{
			name:   "req root collision",
			resMap: map[string]string{},
			reqMap: map[string]string{
				"":  "baz",
				".": "bar",
			},
			passes: false,
		},
	}

	for _, test := range tests {
		_, err := New(OptSetReqMap(test.reqMap), OptSetResMap(test.resMap))
		if err != nil && test.passes {
			t.Errorf("Test %v failed: %v", test.name, err)
		}
		if err == nil && !test.passes {
			t.Errorf("Test %v failed", test.name)
		}
	}
}

func TestTypeMapRequests(t *testing.T) {
	type testCase struct {
		name    string
		input   [][]byte
		output  [][]byte
		skipped []int
	}

	tests := []testCase{
		{
			name: "Single part",
			input: [][]byte{
				[]byte(`{"bar":{"baz":1},"ignored":"keep me"}`),
			},
			output: [][]byte{
				[]byte(`{"foo":{"bar":1}}`),
			},
			skipped: []int{},
		},
		{
			name: "Single part skipped",
			input: [][]byte{
				[]byte(`{"bar":{"baz":1},"ignored":"drop me"}`),
			},
			output:  nil,
			skipped: []int{0},
		},
		{
			name: "Single part bad json",
			input: [][]byte{
				[]byte(` 35234 keep 5$$%@#%`),
			},
			output:  nil,
			skipped: []int{0},
		},
		{
			name:    "Empty",
			input:   [][]byte{},
			output:  nil,
			skipped: []int{},
		},
		{
			name:    "Empty part",
			input:   [][]byte{[]byte(nil)},
			output:  nil,
			skipped: []int{0},
		},
		{
			name: "Multi parts",
			input: [][]byte{
				[]byte(`{"bar":{"baz":2},"ignored":"keep me"}`),
				[]byte(`{"bar":{"baz":3},"ignored":"keep me"}`),
			},
			output: [][]byte{
				[]byte(`{"foo":{"bar":2}}`),
				[]byte(`{"foo":{"bar":3}}`),
			},
			skipped: []int{},
		},
		{
			name: "Multi parts some skipped",
			input: [][]byte{
				[]byte(`{"bar":{"baz":1},"ignored":"keep me"}`),
				[]byte(`{"bar":{"baz":2},"ignored":"drop me"}`),
				[]byte(`{"bar":{"baz":3},"ignored":"keep me"}`),
			},
			output: [][]byte{
				[]byte(`{"foo":{"bar":1}}`),
				[]byte(`{"foo":{"bar":3}}`),
			},
			skipped: []int{1},
		},
		{
			name: "Multi parts some skipped some nil",
			input: [][]byte{
				[]byte(`{"bar":{"baz":1},"ignored":"keep me"}`),
				[]byte(`{"bar":{"baz":2},"ignored":"drop me"}`),
				[]byte(`{"bar":{"baz":3},"ignored":"keep me"}`),
				nil,
			},
			output: [][]byte{
				[]byte(`{"foo":{"bar":1}}`),
				[]byte(`{"foo":{"bar":3}}`),
			},
			skipped: []int{1, 3},
		},
		{
			name: "Multi parts all skipped",
			input: [][]byte{
				[]byte(`{"bar":{"baz":1},"ignored":"drop me"}`),
				[]byte(`{"bar":{"baz":2},"ignored":"drop me"}`),
				[]byte(`{"bar":{"baz":3},"ignored":"drop me"}`),
			},
			output:  nil,
			skipped: []int{0, 1, 2},
		},
	}

	cConf := condition.NewConfig()
	cConf.Type = "text"
	cConf.Text.Operator = "contains"
	cConf.Text.Arg = "keep"

	cond, err := condition.New(cConf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	e, err := New(OptSetReqMap(map[string]string{
		"foo.bar": "bar.baz",
	}), OptSetConditions([]types.Condition{cond}))
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		res, skipped, err := e.MapRequests(message.New(test.input))
		if err != nil {
			t.Errorf("Test '%v' failed: %v", test.name, err)
			continue
		}
		if act, exp := skipped, test.skipped; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong skipped slice for test '%v': %v != %v", test.name, act, exp)
		}
		if act, exp := message.GetAllBytes(res), test.output; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong output for test '%v': %s != %s", test.name, act, exp)
		}
	}
}

func TestMapRequestsParallel(t *testing.T) {
	N := 100

	inputMsg := message.New([][]byte{
		[]byte(`{"foo":{"bar":"baz"}}`),
	})
	inputMsg.Iter(func(i int, p types.Part) error {
		_, _ = p.JSON()
		_ = p.Metadata()
		return nil
	})
	expMsg := [][]byte{
		[]byte(`{"new":{"bar":"baz"}}`),
	}

	wg := sync.WaitGroup{}
	wg.Add(N)

	launchChan := make(chan struct{})

	for i := 0; i < N; i++ {
		cConf := condition.NewConfig()
		cConf.Type = "jmespath"
		cConf.JMESPath.Query = "foo.bar == 'baz'"

		cond, err := condition.New(cConf, types.NoopMgr(), log.Noop(), metrics.Noop())
		if err != nil {
			t.Fatal(err)
		}

		e, err := New(OptSetReqMap(map[string]string{
			"new.bar": "foo.bar",
		}), OptSetConditions([]types.Condition{cond}))
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			<-launchChan
			defer wg.Done()

			res, skipped, err := e.MapRequests(inputMsg)
			if err != nil {
				t.Errorf("failed: %v", err)
			}
			if act, exp := skipped, []int{}; !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong skipped slice: %v != %v", act, exp)
			}
			if act, exp := message.GetAllBytes(res), expMsg; !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong output: %s != %s", act, exp)
			}
		}()
	}

	close(launchChan)
	wg.Wait()
}

func TestTypeAlignResult(t *testing.T) {
	type testCase struct {
		name    string
		length  int
		skipped []int
		input   [][][]byte
		output  [][]byte
	}

	tests := []testCase{
		{
			name:    "single message no skipped",
			length:  3,
			skipped: nil,
			input: [][][]byte{
				{
					[]byte(`foo`),
					[]byte(`bar`),
					[]byte(`baz`),
				},
			},
			output: [][]byte{
				[]byte(`foo`),
				[]byte(`bar`),
				[]byte(`baz`),
			},
		},
		{
			name:    "single message skipped",
			length:  3,
			skipped: []int{1},
			input: [][][]byte{
				{
					[]byte(`foo`),
					[]byte(`baz`),
				},
			},
			output: [][]byte{
				[]byte(`foo`),
				nil,
				[]byte(`baz`),
			},
		},
		{
			name:    "single message lots skipped",
			length:  8,
			skipped: []int{0, 1, 2, 4, 5, 7},
			input: [][][]byte{
				{
					[]byte(`foo`),
					[]byte(`baz`),
				},
			},
			output: [][]byte{
				nil, nil, nil,
				[]byte(`foo`),
				nil, nil,
				[]byte(`baz`),
				nil,
			},
		},
		{
			name:    "multi message skipped",
			length:  6,
			skipped: []int{1, 4},
			input: [][][]byte{
				{
					[]byte(`foo`),
					[]byte(`baz`),
				},
				{
					[]byte(`foo2`),
					[]byte(`baz2`),
				},
			},
			output: [][]byte{
				[]byte(`foo`),
				nil,
				[]byte(`baz`),
				[]byte(`foo2`),
				nil,
				[]byte(`baz2`),
			},
		},
		{
			name:    "multi message lots skipped",
			length:  8,
			skipped: []int{0, 1, 2, 4, 6, 7},
			input: [][][]byte{
				{
					[]byte(`foo`),
				},
				{
					[]byte(`baz`),
				},
			},
			output: [][]byte{
				nil, nil, nil,
				[]byte(`foo`),
				nil,
				[]byte(`baz`),
				nil, nil,
			},
		},
	}

	e, err := New()
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		var input []types.Message
		for _, p := range test.input {
			input = append(input, message.New(p))
		}
		msg, err := e.AlignResult(test.length, test.skipped, input)
		if err != nil {
			t.Errorf("Error '%v': %v", test.name, err)
			continue
		}
		if act, exp := message.GetAllBytes(msg), test.output; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong output for test '%v': %s != %s", test.name, act, exp)
		}
	}
}

func TestTypeMapRequest(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"zip":"old"}`),
	})
	var res types.Message
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"foo":{"bar":1},"zip":"old"}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	e, err = New(OptSetReqMap(map[string]string{
		".": "foo",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg = message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"zip":"old"}`),
	})
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{"zip":"old"}`),
	})
	var skipped []int
	if _, skipped, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := []int{0}, skipped; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	e, err = New(OptSetReqMap(map[string]string{
		".":   "foo",
		"bar": "baz",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg = message.New([][]byte{
		[]byte(`{"foo":{"bar":1,"preserve":true},"baz":"baz value"}`),
	})
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":"baz value","preserve":true}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	e, err = New(OptSetReqMap(map[string]string{
		"foo": "",
		"bar": "baz",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg = message.New([][]byte{
		[]byte(`{"foo":{"bar":1,"preserve":true},"baz":"baz value"}`),
	})
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":"baz value","foo":{"baz":"baz value","foo":{"bar":1,"preserve":true}}}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestTypeMapRequestMetadata(t *testing.T) {
	condConf := condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "bar"

	cond, err := condition.New(condConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	e, err := New(
		OptSetConditions([]types.Condition{cond}),
		OptSetOptReqMap(map[string]string{
			"test": "test",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`{"test":"foo message"}`),
		[]byte(`{"test":"bar message"}`),
		[]byte(`{"test":"foo message"}`),
		[]byte(`{"test":"baz bar message"}`),
		[]byte(`{"test":"foo message"}`),
	})
	msg.Get(0).Metadata().Set("test", "foo")
	msg.Get(1).Metadata().Set("test", "bar")
	msg.Get(2).Metadata().Set("test", "foo")
	msg.Get(3).Metadata().Set("test", "baz")
	msg.Get(5).Metadata().Set("test", "foo")

	var res types.Message
	var skipped []int
	if res, skipped, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, res.Len(); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}

	if exp, act := []int{0, 2, 4}, skipped; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}

	if exp, act := `{"test":"bar message"}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}
	if exp, act := `{"test":"baz bar message"}`, string(res.Get(1).Get()); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}

	if exp, act := "bar", res.Get(0).Metadata().Get("test"); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}
	if exp, act := "baz", res.Get(1).Metadata().Get("test"); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}
}

func TestTypeMapOptRequest(t *testing.T) {
	e, err := New(OptSetOptReqMap(map[string]string{
		".": "foo",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"zip":"old"}`),
	})
	var res types.Message
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{"zip":"old"}`),
	})
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	e, err = New(OptSetOptReqMap(map[string]string{
		"foo": "foo",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg = message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"zip":"old"}`),
	})
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"foo":{"bar":1}}`, string(res.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestTypeOverlayResult(t *testing.T) {
	e, err := New(OptSetResMap(map[string]string{
		"bar": "foo.bar",
		"baz": "bar.baz",
	}), OptSetOptResMap(map[string]string{
		"qux": "baz.qux",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`{}`),
	})
	msg.Get(0).Metadata().Set("foo", "bar")
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"bar":{"baz":2}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1,"baz":2}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `bar`, msg.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{}`),
		[]byte(`{}`),
	})
	msg.Get(0).Metadata().Set("foo", "bar1")
	msg.Get(1).Metadata().Set("foo", "bar2")
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"bar":{"baz":2}}`),
		[]byte(`{"foo":{"bar":3},"bar":{"baz":4},"baz":{"qux":5}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1,"baz":2}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `{"bar":3,"baz":4,"qux":5}`, string(msg.Get(1).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `bar1`, msg.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `bar2`, msg.Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`not %#%$ valid json`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`not valid json`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `not valid json`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{}`),
		[]byte(`{}`),
		[]byte(`{}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"bar":{"baz":2}}`),
		nil,
		[]byte(`{"foo":{"bar":3},"bar":{"baz":4}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1,"baz":2}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `{}`, string(msg.Get(1).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `{"bar":3,"baz":4}`, string(msg.Get(2).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{"bar":"old"}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":"old"}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestTypeOverlayResultRoot(t *testing.T) {
	e, err := New(OptSetResMap(map[string]string{
		"": "foo.bar",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`{"this":"should be removed"}`),
	})
	msg.Get(0).Metadata().Set("foo", "bar1")
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":{"new":"root"}},"bar":{"baz":2}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"new":"root"}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `bar1`, msg.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	e, err = New(OptSetOptResMap(map[string]string{
		"": "foo.bar",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg = message.New([][]byte{
		[]byte(`{"this":"should be removed"}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":{"new":"root"}},"bar":{"baz":2}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"new":"root"}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	e, err = New()
	if err != nil {
		t.Fatal(err)
	}

	msg = message.New([][]byte{
		[]byte(`{"zip":"original"}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":{"new":"root"}},"bar":{"baz":2}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"foo":{"bar":{"new":"root"}},"bar":{"baz":2}}`, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestTypeOverlayResultMisaligned(t *testing.T) {
	e, err := New(OptSetResMap(map[string]string{
		"": "foo.bar",
	}))
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`{"this":"should be removed"}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":{"new":"root"}},"bar":{"baz":2}}`),
		[]byte(`{"this":"should be removed"}`),
	})); err == nil {
		t.Error("Expected error from misaligned batches")
	}
}

func BenchmarkMapRequests(b *testing.B) {
	cConf := condition.NewConfig()
	cConf.Type = "jmespath"
	cConf.JMESPath.Query = "keys(@) == ['foo']"

	cond, err := condition.New(cConf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		b.Fatal(err)
	}

	e, err := New(OptSetReqMap(map[string]string{
		"bar": "foo.input.stdin.delimiter",
	}), OptSetConditions([]types.Condition{cond}))
	if err != nil {
		b.Fatal(err)
	}

	msg := message.New(nil)
	for i := 0; i < b.N; i++ {
		msg.Append(message.NewPart([]byte(`{"foo":{"http":{"address":"0.0.0.0:4195","read_timeout_ms":5000,"root_path":"/benthos","debug_endpoints":false},"input":{"type":"stdin","stdin":{"delimiter":"","max_buffer":1000000,"multipart":false}},"buffer":{"type":"none","none":{}},"pipeline":{"processors":[{"type":"process_dag","process_dag":{}}],"threads":1},"output":{"type":"stdout","stdout":{"delimiter":""}},"resources":{"caches":{},"conditions":{},"rate_limits":{}},"logger":{"prefix":"benthos","level":"INFO","add_timestamp":true,"json_format":true,"static_fields":{"@service":"benthos"}},"metrics":{"type":"http_server","prefix":"benthos","http_server":{},"prometheus":{},"statsd":{"address":"localhost:4040","flush_period":"100ms","network":"udp"}}}}`)))
	}
	msg.Iter(func(i int, p types.Part) error {
		_, _ = p.JSON()
		_ = p.Metadata()
		return nil
	})

	b.ReportAllocs()
	b.ResetTimer()
	_, skipped, err := e.MapRequests(msg)
	if err != nil {
		b.Errorf("failed: %v", err)
	}
	if act, exp := skipped, []int{}; !reflect.DeepEqual(exp, act) {
		b.Errorf("Wrong skipped slice: %v != %v", act, exp)
	}
}

func BenchmarkTypeOverlayResult(b *testing.B) {
	e, err := New(OptSetResMap(map[string]string{
		"foo": "foo",
		"bar": "bar",
	}))
	if err != nil {
		b.Fatal(err)
	}

	msg := message.New(nil)
	overlay := message.New(nil)
	for i := 0; i < b.N; i++ {
		msg.Append(message.NewPart([]byte(`{"http":{"address":"0.0.0.0:4195","read_timeout_ms":5000,"root_path":"/benthos","debug_endpoints":false},"input":{"type":"stdin","stdin":{"delimiter":"","max_buffer":1000000,"multipart":false}},"buffer":{"type":"none","none":{}},"pipeline":{"processors":[{"type":"process_dag","process_dag":{}}],"threads":1},"output":{"type":"stdout","stdout":{"delimiter":""}},"resources":{"caches":{},"conditions":{},"rate_limits":{}},"logger":{"prefix":"benthos","level":"INFO","add_timestamp":true,"json_format":true,"static_fields":{"@service":"benthos"}},"metrics":{"type":"http_server","prefix":"benthos","http_server":{},"prometheus":{},"statsd":{"address":"localhost:4040","flush_period":"100ms","network":"udp"}}}`)))
		overlay.Append(message.NewPart([]byte(`{
			"foo":{"http":{"address":"0.0.0.0:4195","read_timeout_ms":5000,"root_path":"/benthos","debug_endpoints":false},"input":{"type":"stdin","stdin":{"delimiter":"","max_buffer":1000000,"multipart":false}},"buffer":{"type":"none","none":{}},"pipeline":{"processors":[{"type":"process_dag","process_dag":{}}],"threads":1},"output":{"type":"stdout","stdout":{"delimiter":""}},"resources":{"caches":{},"conditions":{},"rate_limits":{}},"logger":{"prefix":"benthos","level":"INFO","add_timestamp":true,"json_format":true,"static_fields":{"@service":"benthos"}},"metrics":{"type":"http_server","prefix":"benthos","http_server":{},"prometheus":{},"statsd":{"address":"localhost:4040","flush_period":"100ms","network":"udp"}}},
			"bar":{"http":{"address":"0.0.0.0:4195","read_timeout_ms":5000,"root_path":"/benthos","debug_endpoints":false},"input":{"type":"stdin","stdin":{"delimiter":"","max_buffer":1000000,"multipart":false}},"buffer":{"type":"none","none":{}},"pipeline":{"processors":[{"type":"process_dag","process_dag":{}}],"threads":1},"output":{"type":"stdout","stdout":{"delimiter":""}},"resources":{"caches":{},"conditions":{},"rate_limits":{}},"logger":{"prefix":"benthos","level":"INFO","add_timestamp":true,"json_format":true,"static_fields":{"@service":"benthos"}},"metrics":{"type":"http_server","prefix":"benthos","http_server":{},"prometheus":{},"statsd":{"address":"localhost:4040","flush_period":"100ms","network":"udp"}}}
		}`)))
	}

	// Pre-marshal the documents as JSON.
	msg.Iter(func(i int, p types.Part) error {
		if _, err = p.JSON(); err != nil {
			b.Fatal(err)
		}
		return nil
	})
	overlay.Iter(func(i int, p types.Part) error {
		if _, err = p.JSON(); err != nil {
			b.Fatal(err)
		}
		return nil
	})

	b.ReportAllocs()
	b.ResetTimer()
	if err = e.MapResponses(msg, overlay); err != nil {
		b.Fatal(err)
	}
}
