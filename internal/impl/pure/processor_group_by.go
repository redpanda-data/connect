package pure

import (
	"context"
	"errors"
	"fmt"
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
		p, err := newGroupBy(conf.GroupBy, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("group_by", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "group_by",
		Categories: []string{
			"Composition",
		},
		Summary: `
Splits a [batch of messages](/docs/configuration/batching) into N batches, where each resulting batch contains a group of messages determined by a [Bloblang query](/docs/guides/bloblang/about).`,
		Description: `
Once the groups are established a list of processors are applied to their respective grouped batch, which can be used to label the batch as per their grouping. Messages that do not pass the check of any specified group are placed in their own group.

The functionality of this processor depends on being applied across messages that are batched. You can find out more about batching [in this doc](/docs/configuration/batching).`,
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Grouped Processing",
				Summary: "Imagine we have a batch of messages that we wish to split into a group of foos and everything else, which should be sent to different output destinations based on those groupings. We also need to send the foos as a tar gzip archive. For this purpose we can use the `group_by` processor with a [`switch`](/docs/components/outputs/switch) output:",
				Config: `
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
			},
		},
		Config: docs.FieldComponent().Array().WithChildren(
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about) that should return a boolean value indicating whether a message belongs to a given group.",
				`this.type == "foo"`,
				`this.contents.urls.contains("https://benthos.dev/")`,
				`true`,
			).HasDefault(""),
			docs.FieldProcessor(
				"processors",
				"A list of [processors](/docs/components/processors/about) to execute on the newly formed group.",
			).HasDefault([]any{}).Array(),
		),
	})
	if err != nil {
		panic(err)
	}
}

type group struct {
	Check      *mapping.Executor
	Processors []processor.V1
}

type groupByProc struct {
	log    log.Modular
	groups []group
}

func newGroupBy(conf processor.GroupByConfig, mgr bundle.NewManagement) (processor.V2Batched, error) {
	var err error
	groups := make([]group, len(conf))

	for i, gConf := range conf {
		if len(gConf.Check) > 0 {
			if groups[i].Check, err = mgr.BloblEnvironment().NewMapping(gConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse check for group '%v': %v", i, err)
			}
		} else {
			return nil, errors.New("a group definition must have a check query")
		}

		for j, pConf := range gConf.Processors {
			pMgr := mgr.IntoPath("group_by", strconv.Itoa(i), "processors", strconv.Itoa(j))
			proc, err := pMgr.NewProcessor(pConf)
			if err != nil {
				return nil, err
			}
			groups[i].Processors = append(groups[i].Processors, proc)
		}
	}

	return &groupByProc{
		log:    mgr.Logger(),
		groups: groups,
	}, nil
}

func (g *groupByProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
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
				g.log.Errorf("Failed to test group %v: %v\n", j, err)
			}
			if res {
				groupStr := strconv.Itoa(j)
				spans[i].LogKV(
					"event", "grouped",
					"type", groupStr,
				)
				spans[i].SetTag("group", groupStr)
				groups[j] = append(groups[j], p)
				return nil
			}
		}

		spans[i].LogKV(
			"event", "grouped",
			"type", "default",
		)
		spans[i].SetTag("group", "default")
		groupless = append(groupless, p)
		return nil
	})

	msgs := []message.Batch{}
	for i, gmsg := range groups {
		if gmsg.Len() == 0 {
			continue
		}

		resultMsgs, res := processor.ExecuteAll(ctx, g.groups[i].Processors, gmsg)
		if len(resultMsgs) > 0 {
			msgs = append(msgs, resultMsgs...)
		}
		if res != nil {
			if err := res; err != nil {
				g.log.Errorf("Processor error: %v\n", err)
			}
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
