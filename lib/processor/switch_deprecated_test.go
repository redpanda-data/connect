package processor

import (
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	"github.com/stretchr/testify/assert"
)

func TestSwitchDeprecatedCases(t *testing.T) {
	conf := NewConfig()
	conf.Type = "switch"

	condConf := condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains_cs"
	condConf.Text.Arg = "A"

	procConf := NewConfig()
	procConf.Type = TypeText
	procConf.Text.Operator = "prepend"
	procConf.Text.Value = "Hit case 0: "

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: false,
	})

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains_cs"
	condConf.Text.Arg = "B"

	procConf = NewConfig()
	procConf.Type = TypeText
	procConf.Text.Operator = "prepend"
	procConf.Text.Value = "Hit case 1: "

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: true,
	})

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains_cs"
	condConf.Text.Arg = "C"

	procConf = NewConfig()
	procConf.Type = TypeText
	procConf.Text.Operator = "prepend"
	procConf.Text.Value = "Hit case 2: "

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: false,
	})

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains_cs"
	condConf.Text.Arg = "D"

	procConf = NewConfig()
	procConf.Type = TypeText
	procConf.Text.Operator = "prepend"
	procConf.Text.Value = "Hit case 3: "

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: false,
	})

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c.CloseAsync()
		if err = c.WaitForClose(time.Second); err != nil {
			t.Fatal(err)
		}
	}()

	type testCase struct {
		name     string
		input    [][]byte
		expected [][]byte
	}
	tests := []testCase{
		{
			name: "switch test 1",
			input: [][]byte{
				[]byte("A"),
				[]byte("AB"),
			},
			expected: [][]byte{
				[]byte("Hit case 0: A"),
				[]byte("Hit case 0: AB"),
			},
		},
		{
			name: "switch test 2",
			input: [][]byte{
				[]byte("B"),
				[]byte("BD"),
			},
			expected: [][]byte{
				[]byte("Hit case 2: Hit case 1: B"),
				[]byte("Hit case 2: Hit case 1: BD"),
			},
		},
		{
			name: "switch test 3",
			input: [][]byte{
				[]byte("C"),
				[]byte("CD"),
			},
			expected: [][]byte{
				[]byte("Hit case 2: C"),
				[]byte("Hit case 2: CD"),
			},
		},
		{
			name: "switch test 4",
			input: [][]byte{
				[]byte("D"),
			},
			expected: [][]byte{
				[]byte("Hit case 3: D"),
			},
		},
	}

	for _, test := range tests {
		msg, res := c.ProcessMessage(message.New(test.input))
		if res != nil {
			t.Error(res.Error())
			continue
		}
		if act, exp := message.GetAllBytes(msg[0]), test.expected; !reflect.DeepEqual(act, exp) {
			t.Errorf("Wrong result for test '%s': %s != %s", test.name, act, exp)
		}
	}
}

func TestSwitchDeprecatedSanitised(t *testing.T) {
	conf := NewConfig()
	conf.Type = "switch"

	condConf := condition.NewConfig()
	condConf.Type = condition.TypeBloblang
	condConf.Bloblang = `this.contains("A")`

	procConf := NewConfig()
	procConf.Type = TypeBloblang
	procConf.Bloblang = `root = "Hit case 0:" + content().string()`

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: false,
	})

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeBloblang
	condConf.Bloblang = `this.contains("B")`

	procConf = NewConfig()
	procConf.Type = TypeBloblang
	procConf.Bloblang = `root = "Hit case 1:" + content().string()`

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: true,
	})

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeBloblang
	condConf.Bloblang = `this.contains("C")`

	procConf = NewConfig()
	procConf.Type = TypeBloblang
	procConf.Bloblang = `root = "Hit case 2:" + content().string()`

	conf.Switch = append(conf.Switch, SwitchCaseConfig{
		Condition:   condConf,
		Processors:  []Config{procConf},
		Fallthrough: false,
	})

	sanit, err := SanitiseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}

	sanitBytes, err := config.MarshalYAML(sanit)
	if err != nil {
		t.Fatal(err)
	}

	exp := `type: switch
switch:
  - check: ""
    condition:
      bloblang: this.contains("A")
      type: bloblang
    fallthrough: false
    processors:
      - bloblang: root = "Hit case 0:" + content().string()
        type: bloblang
  - check: ""
    condition:
      bloblang: this.contains("B")
      type: bloblang
    fallthrough: true
    processors:
      - bloblang: root = "Hit case 1:" + content().string()
        type: bloblang
  - check: ""
    condition:
      bloblang: this.contains("C")
      type: bloblang
    fallthrough: false
    processors:
      - bloblang: root = "Hit case 2:" + content().string()
        type: bloblang
`

	assert.Equal(t, exp, string(sanitBytes))
}
