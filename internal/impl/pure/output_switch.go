package pure

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/gabs/v2"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

var (
	// ErrSwitchNoConditionMet is returned when a message does not match any
	// output conditions.
	ErrSwitchNoConditionMet = errors.New("no switch output conditions were met by message")
	// ErrSwitchNoCasesMatched is returned when a message does not match any
	// output cases.
	ErrSwitchNoCasesMatched = errors.New("no switch cases were matched by message")
	// ErrSwitchNoOutputs is returned when creating a switchOutput type with less than
	// 2 outputs.
	ErrSwitchNoOutputs = errors.New("attempting to create switch with fewer than 2 cases")
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newSwitchOutput(c.Switch, nm)
	}), docs.ComponentSpec{
		Name: "switch",
		Summary: `
The switch output type allows you to route messages to different outputs based on their contents.`,
		Description: `
Messages must successfully route to one or more outputs, otherwise this is considered an error and the message is reprocessed. In order to explicitly drop messages that do not match your cases add one final case with a [drop output](/docs/components/outputs/drop).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldBool(
				"retry_until_success", `
If a selected output fails to send a message this field determines whether it is
reattempted indefinitely. If set to false the error is instead propagated back
to the input level.

If a message can be routed to >1 outputs it is usually best to set this to true
in order to avoid duplicate messages being routed to an output.`,
			).HasDefault(false),
			docs.FieldBool(
				"strict_mode", `
This field determines whether an error should be reported if no condition is met.
If set to true, an error is propagated back to the input level. The default
behavior is false, which will drop the message.`,
			).Advanced().HasDefault(false),
			docs.FieldObject(
				"cases",
				"A list of switch cases, outlining outputs that can be routed to.",
				[]any{
					map[string]any{
						"check": `this.urls.contains("http://benthos.dev")`,
						"output": map[string]any{
							"cache": map[string]any{
								"target": "foo",
								"key":    "${!json(\"id\")}",
							},
						},
						"continue": true,
					},
					map[string]any{
						"output": map[string]any{
							"s3": map[string]any{
								"bucket": "bar",
								"path":   "${!json(\"id\")}",
							},
						},
					},
				},
			).Array().WithChildren(
				docs.FieldBloblang(
					"check",
					"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should be routed to the case output. If left empty the case always passes.",
					`this.type == "foo"`,
					`this.contents.urls.contains("https://benthos.dev/")`,
				).HasDefault(""),
				docs.FieldOutput(
					"output", "An [output](/docs/components/outputs/about/) for messages that pass the check to be routed to.",
				).HasDefault(map[string]any{}),
				docs.FieldBool(
					"continue",
					"Indicates whether, if this case passes for a message, the next case should also be tested.",
				).HasDefault(false).Advanced(),
			).HasDefault([]any{}),
		).LinterFunc(func(ctx docs.LintContext, line, col int, value any) []docs.Lint {
			if _, ok := value.(map[string]any); !ok {
				return nil
			}
			gObj := gabs.Wrap(value)
			retry, exists := gObj.S("retry_until_success").Data().(bool)
			if !exists || !retry {
				return nil
			}
			for _, cObj := range gObj.S("cases").Children() {
				typeStr, _ := cObj.S("output", "type").Data().(string)
				isReject := cObj.Exists("output", "reject")
				if typeStr == "reject" || isReject {
					return []docs.Lint{
						docs.NewLintError(line, docs.LintCustom, "a `switch` output with a `reject` case output must have the field `switch.retry_until_success` set to `false`, otherwise the `reject` child output will result in infinite retries"),
					}
				}
			}
			return nil
		}),
		Categories: []string{
			"Utility",
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "Basic Multiplexing",
				Summary: `
The most common use for a switch output is to multiplex messages across a range of output destinations. The following config checks the contents of the field ` + "`type` of messages and sends `foo` type messages to an `amqp_1` output, `bar` type messages to a `gcp_pubsub` output, and everything else to a `redis_streams` output" + `.

Outputs can have their own processors associated with them, and in this example the ` + "`redis_streams`" + ` output has a processor that enforces the presence of a type field before sending it.`,
				Config: `
output:
  switch:
    cases:
      - check: this.type == "foo"
        output:
          amqp_1:
            url: amqps://guest:guest@localhost:5672/
            target_address: queue:/the_foos

      - check: this.type == "bar"
        output:
          gcp_pubsub:
            project: dealing_with_mike
            topic: mikes_bars

      - output:
          redis_streams:
            url: tcp://localhost:6379
            stream: everything_else
          processors:
            - mapping: |
                root = this
                root.type = this.type | "unknown"
`,
			},
			{
				Title: "Control Flow",
				Summary: `
The ` + "`continue`" + ` field allows messages that have passed a case to be tested against the next one also. This can be useful when combining non-mutually-exclusive case checks.

In the following example a message that passes both the check of the first case as well as the second will be routed to both.`,
				Config: `
output:
  switch:
    cases:
      - check: 'this.user.interests.contains("walks").catch(false)'
        output:
          amqp_1:
            url: amqps://guest:guest@localhost:5672/
            target_address: queue:/people_what_think_good
        continue: true

      - check: 'this.user.dislikes.contains("videogames").catch(false)'
        output:
          gcp_pubsub:
            project: people
            topic: that_i_dont_want_to_hang_with
`,
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

type switchOutput struct {
	logger log.Modular

	transactions <-chan message.Transaction

	strictMode    bool
	outputTSChans []chan message.Transaction
	outputs       []output.Streamed
	checks        []*mapping.Executor
	continues     []bool
	fallthroughs  []bool

	shutSig *shutdown.Signaller
}

func newSwitchOutput(conf output.SwitchConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	o := &switchOutput{
		logger:       mgr.Logger(),
		transactions: nil,
		strictMode:   conf.StrictMode,
		shutSig:      shutdown.NewSignaller(),
	}

	lCases := len(conf.Cases)
	if lCases < 2 {
		return nil, ErrSwitchNoOutputs
	}
	if lCases > 0 {
		o.outputs = make([]output.Streamed, lCases)
		o.checks = make([]*mapping.Executor, lCases)
		o.continues = make([]bool, lCases)
		o.fallthroughs = make([]bool, lCases)
	}

	var err error
	for i, cConf := range conf.Cases {
		oMgr := mgr.IntoPath("switch", strconv.Itoa(i), "output")
		if o.outputs[i], err = oMgr.NewOutput(cConf.Output); err != nil {
			return nil, err
		}
		if conf.RetryUntilSuccess {
			if o.outputs[i], err = RetryOutputIndefinitely(oMgr, o.outputs[i]); err != nil {
				return nil, fmt.Errorf("failed to create case '%v' output type '%v': %v", i, cConf.Output.Type, err)
			}
		}
		if len(cConf.Check) > 0 {
			if o.checks[i], err = mgr.BloblEnvironment().NewMapping(cConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse case '%v' check mapping: %v", i, err)
			}
		}
		o.continues[i] = cConf.Continue
	}

	o.outputTSChans = make([]chan message.Transaction, len(o.outputs))
	for i := range o.outputTSChans {
		o.outputTSChans[i] = make(chan message.Transaction)
		if err := o.outputs[i].Consume(o.outputTSChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (o *switchOutput) Consume(transactions <-chan message.Transaction) error {
	if o.transactions != nil {
		return component.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

func (o *switchOutput) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

func (o *switchOutput) dispatchToTargets(
	group *message.SortGroup,
	sourceMessage message.Batch,
	outputTargets [][]*message.Part,
	ackFn func(context.Context, error) error,
) {
	var setErr func(error)
	var setErrForPart func(*message.Part, error)
	var getErr func() error
	{
		var generalErr error
		var batchErr *batch.Error
		var errLock sync.Mutex

		setErr = func(err error) {
			if err == nil {
				return
			}
			errLock.Lock()
			generalErr = err
			errLock.Unlock()
		}
		setErrForPart = func(part *message.Part, err error) {
			if err == nil {
				return
			}
			errLock.Lock()
			defer errLock.Unlock()

			index := group.GetIndex(part)
			if index == -1 {
				generalErr = err
				return
			}

			if batchErr == nil {
				batchErr = batch.NewError(sourceMessage, err)
			}
			batchErr.Failed(index, err)
		}
		getErr = func() error {
			if batchErr != nil {
				return batchErr
			}
			return generalErr
		}
	}

	var pendingResponses int64
	for _, parts := range outputTargets {
		if len(parts) == 0 {
			continue
		}
		pendingResponses++
	}
	if pendingResponses == 0 {
		ctx, done := o.shutSig.CloseNowCtx(context.Background())
		defer done()
		_ = ackFn(ctx, nil)
	}

	for target, parts := range outputTargets {
		if len(parts) == 0 {
			continue
		}

		i := target
		parts := parts

		select {
		case o.outputTSChans[i] <- message.NewTransactionFunc(parts, func(ctx context.Context, err error) error {
			if err != nil {
				if bErr, ok := err.(*batch.Error); ok {
					bErr.WalkParts(group, sourceMessage, func(i int, p *message.Part, e error) bool {
						if e != nil {
							setErrForPart(p, e)
						}
						return true
					})
				} else {
					for _, p := range parts {
						setErrForPart(p, err)
					}
				}
			}
			if atomic.AddInt64(&pendingResponses, -1) <= 0 {
				return ackFn(ctx, getErr())
			}
			return nil
		}):
		case <-o.shutSig.CloseNowChan():
			setErr(component.ErrTypeClosed)
			return
		}
	}
}

func (o *switchOutput) loop() {
	ackInterruptChan := make(chan struct{})
	var ackPending int64

	defer func() {
		// Wait for pending acks to be resolved, or forceful termination
	ackWaitLoop:
		for atomic.LoadInt64(&ackPending) > 0 {
			select {
			case <-ackInterruptChan:
			case <-time.After(time.Millisecond * 100):
				// Just incase an interrupt doesn't arrive.
			case <-o.shutSig.CloseNowChan():
				break ackWaitLoop
			}
		}
		for _, tChan := range o.outputTSChans {
			close(tChan)
		}
		for _, output := range o.outputs {
			output.TriggerCloseNow()
		}
		for _, output := range o.outputs {
			_ = output.WaitForClose(context.Background())
		}
		o.shutSig.ShutdownComplete()
	}()

	shutCtx, done := o.shutSig.CloseNowCtx(context.Background())
	defer done()

	for !o.shutSig.ShouldCloseNow() {
		var ts message.Transaction
		var open bool

		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.shutSig.CloseNowChan():
			return
		}

		group, trackedMsg := message.NewSortGroup(ts.Payload)

		outputTargets := make([][]*message.Part, len(o.checks))
		if checksErr := trackedMsg.Iter(func(i int, p *message.Part) error {
			routedAtLeastOnce := false
			for j, exe := range o.checks {
				test := true
				if exe != nil {
					var err error
					if test, err = exe.QueryPart(i, trackedMsg); err != nil {
						test = false
						o.logger.Errorf("Failed to test case %v: %v\n", j, err)
					}
				}
				if test {
					routedAtLeastOnce = true
					outputTargets[j] = append(outputTargets[j], p.ShallowCopy())
					if !o.continues[j] {
						return nil
					}
				}
			}
			if !routedAtLeastOnce && o.strictMode {
				return ErrSwitchNoConditionMet
			}
			return nil
		}); checksErr != nil {
			if err := ts.Ack(shutCtx, checksErr); err != nil && shutCtx.Err() != nil {
				return
			}
			continue
		}

		_ = atomic.AddInt64(&ackPending, 1)
		o.dispatchToTargets(group, trackedMsg, outputTargets, func(ctx context.Context, err error) error {
			ackErr := ts.Ack(ctx, err)
			_ = atomic.AddInt64(&ackPending, -1)
			select {
			case ackInterruptChan <- struct{}{}:
			default:
			}
			return ackErr
		})
	}
}

func (o *switchOutput) TriggerCloseNow() {
	o.shutSig.CloseNow()
}

func (o *switchOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-o.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
