package processor

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	imessage "github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSwitch] = TypeSpec{
		constructor: NewSwitch,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
Conditionally processes messages based on their contents.`,
		Description: `
For each switch case a [Bloblang query](/docs/guides/bloblang/about/) is checked and, if the result is true (or the check is empty) the child processors are executed on the message.`,
		Footnotes: `
## Batching

When a switch processor executes on a [batch of messages](/docs/configuration/batching/) they are checked individually and can be matched independently against cases. During processing the messages matched against a case are processed as a batch, although the ordering of messages during case processing cannot be guaranteed to match the order as received.

At the end of switch processing the resulting batch will follow the same ordering as the batch was received. If any child processors have split or otherwise grouped messages this grouping will be lost as the result of a switch is always a single batch. In order to perform conditional grouping and/or splitting use the [` + "`group_by`" + ` processor](/docs/components/processors/group_by/).`,
		config: docs.FieldComponent().Array().WithChildren(
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should have the processors of this case executed on it. If left empty the case always passes. If the check mapping throws an error the message will be flagged [as having failed](/docs/configuration/error_handling) and will not be tested against any other cases.",
				`this.type == "foo"`,
				`this.contents.urls.contains("https://benthos.dev/")`,
			).HasDefault(""),
			docs.FieldCommon(
				"processors",
				"A list of [processors](/docs/components/processors/about/) to execute on a message.",
			).HasDefault([]interface{}{}).Array().HasType(docs.FieldTypeProcessor),
			docs.FieldAdvanced(
				"fallthrough",
				"Indicates whether, if this case passes for a message, the next case should also be executed.",
			).HasDefault(false).HasType(docs.FieldTypeBool),
		),
		Examples: []docs.AnnotatedExample{
			{
				Title: "I Hate George",
				Summary: `
We have a system where we're counting a metric for all messages that pass through our system. However, occasionally we get messages from George where he's rambling about dumb stuff we don't care about.

For Georges messages we want to instead emit a metric that gauges how angry he is about being ignored and then we drop it.`,
				Config: `
pipeline:
  processors:
    - switch:
        - check: this.user.name.first != "George"
          processors:
            - metric:
                type: counter
                name: MessagesWeCareAbout

        - processors:
            - metric:
                type: gauge
                name: GeorgesAnger
                value: ${! json("user.anger") }
            - bloblang: root = deleted()
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// SwitchCaseConfig contains a condition, processors and other fields for an
// individual case in the Switch processor.
type SwitchCaseConfig struct {
	Check       string   `json:"check" yaml:"check"`
	Processors  []Config `json:"processors" yaml:"processors"`
	Fallthrough bool     `json:"fallthrough" yaml:"fallthrough"`
}

// NewSwitchCaseConfig returns a new SwitchCaseConfig with default values.
func NewSwitchCaseConfig() SwitchCaseConfig {
	return SwitchCaseConfig{
		Check:       "",
		Processors:  []Config{},
		Fallthrough: false,
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

//------------------------------------------------------------------------------

// SwitchConfig is a config struct containing fields for the Switch processor.
type SwitchConfig []SwitchCaseConfig

// NewSwitchConfig returns a default SwitchConfig.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{}
}

//------------------------------------------------------------------------------

// switchCase contains a condition, processors and other fields for an
// individual case in the Switch processor.
type switchCase struct {
	check       *mapping.Executor
	processors  []types.Processor
	fallThrough bool
}

// Switch is a processor that only applies child processors under a certain
// condition.
type Switch struct {
	cases []switchCase
	log   log.Modular

	mCount metrics.StatCounter
	mSent  metrics.StatCounter
}

// NewSwitch returns a Switch processor.
func NewSwitch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var cases []switchCase
	for i, caseConf := range conf.Switch {
		prefix := strconv.Itoa(i)

		var err error
		var check *mapping.Executor
		var procs []types.Processor

		if len(caseConf.Check) > 0 {
			if check, err = interop.NewBloblangMapping(mgr, caseConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse case %v check: %w", i, err)
			}
		}

		if len(caseConf.Processors) == 0 {
			return nil, fmt.Errorf("case [%v] has no processors, in order to have a no-op case use a `noop` processor", i)
		}

		for j, procConf := range caseConf.Processors {
			pMgr, pLog, pStats := interop.LabelChild(prefix+"."+strconv.Itoa(j), mgr, log, stats)
			var proc types.Processor
			if proc, err = New(procConf, pMgr, pLog, pStats); err != nil {
				return nil, fmt.Errorf("case [%v] processor [%v]: %w", i, j, err)
			}
			procs = append(procs, proc)
		}

		cases = append(cases, switchCase{
			check:       check,
			processors:  procs,
			fallThrough: caseConf.Fallthrough,
		})
	}
	return &Switch{
		cases: cases,
		log:   log,

		mCount: stats.GetCounter("count"),
		mSent:  stats.GetCounter("sent"),
	}, nil
}

//------------------------------------------------------------------------------

func reorderFromGroup(group *imessage.SortGroup, parts []types.Part) {
	partToIndex := map[types.Part]int{}
	for _, p := range parts {
		if i := group.GetIndex(p); i >= 0 {
			partToIndex[p] = i
		}
	}

	sort.SliceStable(parts, func(i, j int) bool {
		if index, found := partToIndex[parts[i]]; found {
			i = index
		}
		if index, found := partToIndex[parts[j]]; found {
			j = index
		}
		return i < j
	})
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Switch) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	s.mCount.Incr(1)

	var result []types.Part
	var remaining []types.Part
	var carryOver []types.Part

	sortGroup, sortMsg := imessage.NewSortGroup(msg)
	remaining = make([]types.Part, sortMsg.Len())
	sortMsg.Iter(func(i int, p types.Part) error {
		remaining[i] = p
		return nil
	})

	for i, switchCase := range s.cases {
		passed, failed := carryOver, []types.Part{}

		// Form a message to test against, consisting of fallen through messages
		// from prior cases plus remaining messages that haven't passed a case
		// yet.
		testMsg := message.New(nil)
		testMsg.Append(remaining...)

		for j, p := range remaining {
			test := switchCase.check == nil
			if !test {
				var err error
				if test, err = switchCase.check.QueryPart(j, testMsg); err != nil {
					s.log.Errorf("Failed to test case %v: %v\n", i, err)
					FlagErr(p, err)
					result = append(result, p)
					continue
				}
			}
			if test {
				passed = append(passed, p)
			} else {
				failed = append(failed, p)
			}
		}

		carryOver = nil
		remaining = failed

		if len(passed) > 0 {
			execMsg := message.New(nil)
			execMsg.SetAll(passed)

			msgs, res := ExecuteAll(switchCase.processors, execMsg)
			if res != nil && res.Error() != nil {
				return nil, res
			}

			for _, m := range msgs {
				m.Iter(func(_ int, p types.Part) error {
					if switchCase.fallThrough {
						carryOver = append(carryOver, p)
					} else {
						result = append(result, p)
					}
					return nil
				})
			}
		}
	}

	result = append(result, remaining...)
	if len(result) > 1 {
		reorderFromGroup(sortGroup, result)
	}

	resMsg := message.New(nil)
	resMsg.SetAll(result)

	if resMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	s.mSent.Incr(int64(resMsg.Len()))
	return []types.Message{resMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Switch) CloseAsync() {
	for _, s := range s.cases {
		for _, proc := range s.processors {
			proc.CloseAsync()
		}
	}
}

// WaitForClose blocks until the processor has closed down.
func (s *Switch) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, s := range s.cases {
		for _, proc := range s.processors {
			if err := proc.WaitForClose(time.Until(stopBy)); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
