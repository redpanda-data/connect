package pure

import (
	"context"
	"fmt"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	gbpFieldCheck      = "check"
	gbpFieldProcessors = "processors"
)

func groupByProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Stable().
		Summary(`Splits a [batch of messages](/docs/configuration/batching) into N batches, where each resulting batch contains a group of messages determined by a [Bloblang query](/docs/guides/bloblang/about).`).
		Description(`
Once the groups are established a list of processors are applied to their respective grouped batch, which can be used to label the batch as per their grouping. Messages that do not pass the check of any specified group are placed in their own group.

The functionality of this processor depends on being applied across messages that are batched. You can find out more about batching [in this doc](/docs/configuration/batching).`).
		Example(
			"Grouped Processing",
			"Imagine we have a batch of messages that we wish to split into a group of foos and everything else, which should be sent to different output destinations based on those groupings. We also need to send the foos as a tar gzip archive. For this purpose we can use the `group_by` processor with a [`switch`](/docs/components/outputs/switch) output:",
			`
pipeline:
  processors:
    - group_by:
      - check: content().contains("this is a foo")
        processors:
          - archive:
              format: tar
          - compress:
              algorithm: gzip
          - mapping: 'meta grouping = "foo"'

output:
  switch:
    cases:
      - check: meta("grouping") == "foo"
        output:
          gcp_pubsub:
            project: foo_prod
            topic: only_the_foos
      - output:
          gcp_pubsub:
            project: somewhere_else
            topic: no_foos_here
`,
		).
		Field(service.NewObjectListField("",
			service.NewBloblangField(gbpFieldCheck).
				Description("A [Bloblang query](/docs/guides/bloblang/about) that should return a boolean value indicating whether a message belongs to a given group.").
				Examples(
					`this.type == "foo"`,
					`this.contents.urls.contains("https://benthos.dev/")`,
					`true`,
				),
			service.NewProcessorListField(gbpFieldProcessors).
				Description("A list of [processors](/docs/components/processors/about) to execute on the newly formed group.").
				Default([]any{}),
		))
}

func init() {
	err := service.RegisterBatchProcessor(
		"group_by", groupByProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			groupConfs, err := conf.FieldObjectList()
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p := &groupByProc{log: mgr.Logger()}
			p.groups = make([]group, len(groupConfs))
			for i, c := range groupConfs {
				if p.groups[i], err = groupFromParsed(c, mgr); err != nil {
					return nil, fmt.Errorf("group '%v' parse error: %w", i, err)
				}
			}

			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("group_by", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type group struct {
	Check      *mapping.Executor
	Processors []processor.V1
}

func groupFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (g group, err error) {
	var checkStr string
	if checkStr, err = conf.FieldString(gbpFieldCheck); err != nil {
		return
	}
	if g.Check, err = mgr.BloblEnvironment().NewMapping(checkStr); err != nil {
		return
	}

	var iProcs []*service.OwnedProcessor
	if iProcs, err = conf.FieldProcessorList(gbpFieldProcessors); err != nil {
		return
	}

	g.Processors = make([]processor.V1, len(iProcs))
	for i, c := range iProcs {
		g.Processors[i] = interop.UnwrapOwnedProcessor(c)
	}
	return
}

type groupByProc struct {
	log    log.Modular
	groups []group
}

func (g *groupByProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	if msg.Len() == 0 {
		return nil, nil
	}

	groups := make([]message.Batch, len(g.groups))
	for i := range groups {
		groups[i] = message.QuickBatch(nil)
	}
	groupless := message.QuickBatch(nil)

	_ = msg.Iter(func(i int, p *message.Part) error {
		for j, group := range g.groups {
			res, err := group.Check.QueryPart(i, msg)
			if err != nil {
				res = false
				g.log.Error("Failed to test group %v: %v\n", j, err)
			}
			if res {
				groupStr := strconv.Itoa(j)
				ctx.Span(i).LogKV("event", "grouped", "type", groupStr)
				ctx.Span(i).SetTag("group", groupStr)
				groups[j] = append(groups[j], p)
				return nil
			}
		}

		ctx.Span(i).LogKV("event", "grouped", "type", "default")
		ctx.Span(i).SetTag("group", "default")
		groupless = append(groupless, p)
		return nil
	})

	msgs := []message.Batch{}
	for i, gmsg := range groups {
		if gmsg.Len() == 0 {
			continue
		}

		resultMsgs, err := processor.ExecuteAll(ctx.Context(), g.groups[i].Processors, gmsg)
		if err != nil {
			return nil, err
		}
		if len(resultMsgs) > 0 {
			msgs = append(msgs, resultMsgs...)
		}
	}
	if groupless.Len() > 0 {
		msgs = append(msgs, groupless)
	}

	if len(msgs) == 0 {
		return nil, nil
	}
	return msgs, nil
}

func (g *groupByProc) Close(ctx context.Context) error {
	for _, group := range g.groups {
		for _, p := range group.Processors {
			if err := p.Close(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
