package mapper

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/types"
)

func TestTypeDeps(t *testing.T) {
	expDeps := []string{
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
			name: "bad res map",
			resMap: map[string]string{
				"":        "baz",
				"foo.baz": "bar",
			},
			reqMap: map[string]string{},
			passes: false,
		},
		{
			name: "bad res map 2",
			resMap: map[string]string{
				"foo.bar":     "baz",
				"foo.bar.baz": "bar",
			},
			reqMap: map[string]string{},
			passes: false,
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
			name:   "bad req map",
			resMap: map[string]string{},
			reqMap: map[string]string{
				"":        "baz",
				"foo.baz": "bar",
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

func TestTypeDo(t *testing.T) {
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
		if act, exp := res.GetAll(), test.output; !reflect.DeepEqual(exp, act) {
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
	if exp, act := `{"foo":{"bar":1},"zip":"old"}`, string(res.Get(0)); exp != act {
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
	if exp, act := `{"bar":1}`, string(res.Get(0)); exp != act {
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
	if exp, act := `{"bar":1}`, string(res.Get(0)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{"zip":"old"}`),
	})
	if res, _, err = e.MapRequests(msg); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{}`, string(res.Get(0)); exp != act {
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
	if exp, act := `{"foo":{"bar":1}}`, string(res.Get(0)); exp != act {
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
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"bar":{"baz":2}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1,"baz":2}`, string(msg.Get(0)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	msg = message.New([][]byte{
		[]byte(`{}`),
		[]byte(`{}`),
	})
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":1},"bar":{"baz":2}}`),
		[]byte(`{"foo":{"bar":3},"bar":{"baz":4},"baz":{"qux":5}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"bar":1,"baz":2}`, string(msg.Get(0)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `{"bar":3,"baz":4,"qux":5}`, string(msg.Get(1)); exp != act {
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
	if exp, act := `{}`, string(msg.Get(0)); exp != act {
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
	if exp, act := `not valid json`, string(msg.Get(0)); exp != act {
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
	if exp, act := `{"bar":1,"baz":2}`, string(msg.Get(0)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `{}`, string(msg.Get(1)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := `{"bar":3,"baz":4}`, string(msg.Get(2)); exp != act {
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
	if exp, act := `{"bar":"old"}`, string(msg.Get(0)); exp != act {
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
	if err = e.MapResponses(msg, message.New([][]byte{
		[]byte(`{"foo":{"bar":{"new":"root"}},"bar":{"baz":2}}`),
	})); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"new":"root"}`, string(msg.Get(0)); exp != act {
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
	if exp, act := `{"new":"root"}`, string(msg.Get(0)); exp != act {
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
	if exp, act := `{"bar":{"baz":2},"foo":{"bar":{"new":"root"}}}`, string(msg.Get(0)); exp != act {
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
