package pure

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newSwitchProc(conf.Switch, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("switch", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "switch",
		Categories: []string{
			"Composition",
		},
		Summary: `
Conditionally processes messages based on their contents.`,
		Description: `
For each switch case a [Bloblang query](/docs/guides/bloblang/about) is checked and, if the result is true (or the check is empty) the child processors are executed on the message.`,
		Footnotes: `
## Batching

When a switch processor executes on a [batch of messages](/docs/configuration/batching) they are checked individually and can be matched independently against cases. During processing the messages matched against a case are processed as a batch, although the ordering of messages during case processing cannot be guaranteed to match the order as received.

At the end of switch processing the resulting batch will follow the same ordering as the batch was received. If any child processors have split or otherwise grouped messages this grouping will be lost as the result of a switch is always a single batch. In order to perform conditional grouping and/or splitting use the [` + "`group_by`" + ` processor](/docs/components/processors/group_by).`,
		Config: docs.FieldComponent().Array().WithChildren(
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about) that should return a boolean value indicating whether a message should have the processors of this case executed on it. If left empty the case always passes. If the check mapping throws an error the message will be flagged [as having failed](/docs/configuration/error_handling) and will not be tested against any other cases.",
				`this.type == "foo"`,
				`this.contents.urls.contains("https://benthos.dev/")`,
			).HasDefault(""),
			docs.FieldProcessor(
				"processors",
				"A list of [processors](/docs/components/processors/about/) to execute on a message.",
			).HasDefault([]any{}).Array(),
			docs.FieldBool(
				"fallthrough",
				"Indicates whether, if this case passes for a message, the next case should also be executed.",
			).HasDefault(false).Advanced(),
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
            - mapping: root = deleted()
`,
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

// switchCase contains a condition, processors and other fields for an
// individual case in the Switch processor.
type switchCase struct {
	check       *mapping.Executor
	processors  []processor.V1
	fallThrough bool
}

type switchProc struct {
	cases []switchCase
	log   log.Modular
}

func newSwitchProc(conf processor.SwitchConfig, mgr bundle.NewManagement) (*switchProc, error) {
	var cases []switchCase
	for i, caseConf := range conf {
		var err error
		var check *mapping.Executor
		var procs []processor.V1

		if len(caseConf.Check) > 0 {
			if check, err = mgr.BloblEnvironment().NewMapping(caseConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse case %v check: %w", i, err)
			}
		}

		if len(caseConf.Processors) == 0 {
			return nil, fmt.Errorf("case [%v] has no processors, in order to have a no-op case use a `noop` processor", i)
		}

		for j, procConf := range caseConf.Processors {
			pMgr := mgr.IntoPath("switch", strconv.Itoa(i), "processors", strconv.Itoa(j))
			proc, err := pMgr.NewProcessor(procConf)
			if err != nil {
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
	return &switchProc{
		cases: cases,
		log:   mgr.Logger(),
	}, nil
}

// SwitchReorderFromGroup takes a message sort group and rearranges a slice of
// message parts so that they match up from their origins.
func SwitchReorderFromGroup(group *message.SortGroup, parts []*message.Part) {
	partToIndex := map[*message.Part]int{}
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

func (s *switchProc) ProcessBatch(ctx context.Context, _ []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	var result []*message.Part
	var remaining []*message.Part
	var carryOver []*message.Part

	sortGroup, sortMsg := message.NewSortGroup(msg)
	remaining = make([]*message.Part, sortMsg.Len())
	_ = sortMsg.Iter(func(i int, p *message.Part) error {
		remaining[i] = p
		return nil
	})

	for i, switchCase := range s.cases {
		passed, failed := carryOver, []*message.Part{}

		// Form a message to test against, consisting of fallen through messages
		// from prior cases plus remaining messages that haven't passed a case
		// yet.
		testMsg := message.Batch(remaining)

		for j, p := range remaining {
			test := switchCase.check == nil
			if !test {
				var err error
				if test, err = switchCase.check.QueryPart(j, testMsg); err != nil {
					s.log.Errorf("Failed to test case %v: %v\n", i, err)
					processor.MarkErr(p, nil, err)
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
			execMsg := message.Batch(passed)

			msgs, res := processor.ExecuteAll(ctx, switchCase.processors, execMsg)
			if res != nil {
				return nil, res
			}

			for _, m := range msgs {
				_ = m.Iter(func(_ int, p *message.Part) error {
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
		SwitchReorderFromGroup(sortGroup, result)
	}

	resMsg := message.Batch(result)
	if resMsg.Len() == 0 {
		return nil, nil
	}

	return []message.Batch{resMsg}, nil
}

func (s *switchProc) Close(ctx context.Context) error {
	for _, c := range s.cases {
		for _, p := range c.processors {
			if err := p.Close(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
