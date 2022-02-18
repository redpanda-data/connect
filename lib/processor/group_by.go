package processor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGroupBy] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newGroupBy(conf.GroupBy, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("group_by", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
Splits a [batch of messages](/docs/configuration/batching/) into N batches, where each resulting batch contains a group of messages determined by a [Bloblang query](/docs/guides/bloblang/about/).`,
		Description: `
Once the groups are established a list of processors are applied to their respective grouped batch, which can be used to label the batch as per their grouping. Messages that do not pass the check of any specified group are placed in their own group.`,
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
          - bloblang: 'meta grouping = "foo"'

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
		config: docs.FieldComponent().Array().WithChildren(
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message belongs to a given group.",
				`this.type == "foo"`,
				`this.contents.urls.contains("https://benthos.dev/")`,
				`true`,
			).HasDefault(""),
			docs.FieldCommon(
				"processors",
				"A list of [processors](/docs/components/processors/about/) to execute on the newly formed group.",
			).HasDefault([]interface{}{}).Array().HasType(docs.FieldTypeProcessor),
		),
		UsesBatches: true,
	}
}

//------------------------------------------------------------------------------

// GroupByElement represents a group determined by a condition and a list of
// group specific processors.
type GroupByElement struct {
	Check      string   `json:"check" yaml:"check"`
	Processors []Config `json:"processors" yaml:"processors"`
}

//------------------------------------------------------------------------------

// GroupByConfig is a configuration struct containing fields for the GroupBy
// processor, which breaks message batches down into N batches of a smaller size
// according to conditions.
type GroupByConfig []GroupByElement

// NewGroupByConfig returns a GroupByConfig with default values.
func NewGroupByConfig() GroupByConfig {
	return GroupByConfig{}
}

//------------------------------------------------------------------------------

type group struct {
	Check      *mapping.Executor
	Processors []processor.V1
}

type groupByProc struct {
	log    log.Modular
	groups []group
}

func newGroupBy(conf GroupByConfig, mgr interop.Manager) (processor.V2Batched, error) {
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
			proc, err := New(pConf, pMgr, pMgr.Logger(), pMgr.Metrics())
			if err != nil {
				return nil, fmt.Errorf("failed to create processor '%v' for group '%v': %v", j, i, err)
			}
			groups[i].Processors = append(groups[i].Processors, proc)
		}
	}

	return &groupByProc{
		log:    mgr.Logger(),
		groups: groups,
	}, nil
}

func (g *groupByProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	if msg.Len() == 0 {
		return nil, nil
	}

	groups := make([]*message.Batch, len(g.groups))
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
				groups[j].Append(p.Copy())
				return nil
			}
		}

		spans[i].LogKV(
			"event", "grouped",
			"type", "default",
		)
		spans[i].SetTag("group", "default")
		groupless.Append(p.Copy())
		return nil
	})

	msgs := []*message.Batch{}
	for i, gmsg := range groups {
		if gmsg.Len() == 0 {
			continue
		}

		resultMsgs, res := ExecuteAll(g.groups[i].Processors, gmsg)
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
			p.CloseAsync()
		}
	}
	deadline, exists := ctx.Deadline()
	if !exists {
		deadline = time.Now().Add(time.Second * 5)
	}
	for _, group := range g.groups {
		for _, p := range group.Processors {
			if err := p.WaitForClose(time.Until(deadline)); err != nil {
				return err
			}
		}
	}
	return nil
}
