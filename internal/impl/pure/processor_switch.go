package pure

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	spFieldCheck       = "check"
	spFieldProcessors  = "processors"
	spFieldFallthrough = "fallthrough"
)

func switchProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Stable().
		Summary(`Conditionally processes messages based on their contents.`).
		Description(`For each switch case a xref:guides:bloblang/about.adoc[Bloblang query] is checked and, if the result is true (or the check is empty) the child processors are executed on the message.`).
		Footnotes(`
== Batching

When a switch processor executes on a xref:configuration:batching.adoc[batch of messages] they are checked individually and can be matched independently against cases. During processing the messages matched against a case are processed as a batch, although the ordering of messages during case processing cannot be guaranteed to match the order as received.

At the end of switch processing the resulting batch will follow the same ordering as the batch was received. If any child processors have split or otherwise grouped messages this grouping will be lost as the result of a switch is always a single batch. In order to perform conditional grouping and/or splitting use the xref:components:processors/group_by.adoc[`+"`group_by`"+` processor].`).
		Example("I Hate George", `
We have a system where we're counting a metric for all messages that pass through our system. However, occasionally we get messages from George where he's rambling about dumb stuff we don't care about.

For Georges messages we want to instead emit a metric that gauges how angry he is about being ignored and then we drop it.`,
			`
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
		).
		Field(service.NewObjectListField("",
			service.NewBloblangField(spFieldCheck).
				Description("A xref:guides:bloblang/about.adoc[Bloblang query] that should return a boolean value indicating whether a message should have the processors of this case executed on it. If left empty the case always passes. If the check mapping throws an error the message will be flagged xref:configuration:error_handling.adoc[as having failed] and will not be tested against any other cases.").
				Examples(
					`this.type == "foo"`,
					`this.contents.urls.contains("https://benthos.dev/")`,
				).
				Default(""),
			service.NewProcessorListField(spFieldProcessors).
				Description("A list of xref:components:processors/about.adoc[processors] to execute on a message.").
				Default([]any{}),
			service.NewBoolField(spFieldFallthrough).
				Description("Indicates whether, if this case passes for a message, the next case should also be executed.").
				Advanced().
				Default(false),
		))
}

func init() {
	err := service.RegisterBatchProcessor(
		"switch", switchProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			caseConfs, err := conf.FieldObjectList()
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p := &switchProc{log: mgr.Logger()}
			p.cases = make([]switchCase, len(caseConfs))
			for i, c := range caseConfs {
				if p.cases[i], err = switchCaseFromParsed(c, mgr); err != nil {
					return nil, fmt.Errorf("case '%v' parse error: %w", i, err)
				}
			}

			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("switch", p, mgr)), nil
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

func switchCaseFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (c switchCase, err error) {
	if checkStr, _ := conf.FieldString(spFieldCheck); checkStr != "" {
		if c.check, err = mgr.BloblEnvironment().NewMapping(checkStr); err != nil {
			return
		}
	}

	c.fallThrough, _ = conf.FieldBool(spFieldFallthrough)

	var iProcs []*service.OwnedProcessor
	if iProcs, err = conf.FieldProcessorList(spFieldProcessors); err != nil {
		return
	}
	if len(iProcs) == 0 {
		err = errors.New("case has no processors, in order to have a no-op case use a `noop` processor")
		return
	}

	c.processors = make([]processor.V1, len(iProcs))
	for i, proc := range iProcs {
		c.processors[i] = interop.UnwrapOwnedProcessor(proc)
	}
	return
}

type switchProc struct {
	cases []switchCase
	log   log.Modular
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

func (s *switchProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
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
					s.log.Error("Failed to test case %v: %v\n", i, err)
					ctx.OnError(fmt.Errorf("failed to test case %v: %w", i, err), -1, p)
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

			msgs, res := processor.ExecuteAll(ctx.Context(), switchCase.processors, execMsg)
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
