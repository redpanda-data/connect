package pure

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	soFieldRetryUntilSuccess = "retry_until_success"
	soFieldStrictMode        = "strict_mode"
	soFieldCases             = "cases"
	soFieldCasesCheck        = "check"
	soFieldCasesContinue     = "continue"
	soFieldCasesOutput       = "output"
)

func switchOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`The switch output type allows you to route messages to different outputs based on their contents.`).
		Description(`Messages that do not pass the check of a single output case are effectively dropped. In order to prevent this outcome set the field `+"[`strict_mode`](#strict_mode) to `true`"+`, in which case messages that do not pass at least one case are considered failed and will be nacked and/or reprocessed depending on your input.`).
		Example(
			"Basic Multiplexing",
			`
The most common use for a switch output is to multiplex messages across a range of output destinations. The following config checks the contents of the field `+"`type` of messages and sends `foo` type messages to an `amqp_1` output, `bar` type messages to a `gcp_pubsub` output, and everything else to a `redis_streams` output"+`.

Outputs can have their own processors associated with them, and in this example the `+"`redis_streams`"+` output has a processor that enforces the presence of a type field before sending it.`,
			`
output:
  switch:
    cases:
      - check: this.type == "foo"
        output:
          amqp_1:
            urls: [ amqps://guest:guest@localhost:5672/ ]
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
		).
		Example(
			"Control Flow",
			`
The `+"`continue`"+` field allows messages that have passed a case to be tested against the next one also. This can be useful when combining non-mutually-exclusive case checks.

In the following example a message that passes both the check of the first case as well as the second will be routed to both.`,
			`
output:
  switch:
    cases:
      - check: 'this.user.interests.contains("walks").catch(false)'
        output:
          amqp_1:
            urls: [ amqps://guest:guest@localhost:5672/ ]
            target_address: queue:/people_what_think_good
        continue: true

      - check: 'this.user.dislikes.contains("videogames").catch(false)'
        output:
          gcp_pubsub:
            project: people
            topic: that_i_dont_want_to_hang_with
`,
		).
		LintRule(`if this.exists("retry_until_success") && this.retry_until_success {
  if this.cases.or([]).any(oconf -> oconf.output.type.or("") == "reject" || oconf.output.reject.type() == "string" ) {
    "a 'switch' output with a 'reject' case output must have the field 'switch.retry_until_success' set to 'false', otherwise the 'reject' child output will result in infinite retries"
  }
}`).
		Fields(
			service.NewBoolField(soFieldRetryUntilSuccess).
				Description(`
If a selected output fails to send a message this field determines whether it is reattempted indefinitely. If set to false the error is instead propagated back to the input level.

If a message can be routed to >1 outputs it is usually best to set this to true in order to avoid duplicate messages being routed to an output.`).
				Default(false),
			service.NewBoolField(soFieldStrictMode).
				Description(`This field determines whether an error should be reported if no condition is met. If set to true, an error is propagated back to the input level. The default behavior is false, which will drop the message.`).
				Advanced().
				Default(false),
			service.NewObjectListField(soFieldCases,
				service.NewBloblangField(soFieldCasesCheck).
					Description("A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should be routed to the case output. If left empty the case always passes.").
					Examples(
						`this.type == "foo"`,
						`this.contents.urls.contains("https://benthos.dev/")`,
					).
					Default(""),
				service.NewOutputField(soFieldCasesOutput).
					Description("An [output](/docs/components/outputs/about/) for messages that pass the check to be routed to."),
				service.NewBoolField(soFieldCasesContinue).
					Description("Indicates whether, if this case passes for a message, the next case should also be tested.").
					Default(false).
					Advanced(),
			).
				Description("A list of switch cases, outlining outputs that can be routed to.").
				Example([]any{
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
				}),
		)
}

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
	err := service.RegisterBatchOutput(
		"switch", switchOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1

			var s output.Streamed
			if s, err = switchOutputFromParsed(conf, interop.UnwrapManagement(mgr)); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(s)
			return
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

func switchOutputFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (*switchOutput, error) {
	strictMode, err := conf.FieldBool(soFieldStrictMode)
	if err != nil {
		return nil, err
	}

	retryUntilSuccess, err := conf.FieldBool(soFieldRetryUntilSuccess)
	if err != nil {
		return nil, err
	}

	cases, err := conf.FieldObjectList(soFieldCases)
	if err != nil {
		return nil, err
	}

	o := &switchOutput{
		logger:       mgr.Logger(),
		transactions: nil,
		strictMode:   strictMode,
		shutSig:      shutdown.NewSignaller(),
	}

	lCases := len(cases)
	if lCases < 2 {
		return nil, ErrSwitchNoOutputs
	}
	if lCases > 0 {
		o.outputs = make([]output.Streamed, lCases)
		o.checks = make([]*mapping.Executor, lCases)
		o.continues = make([]bool, lCases)
		o.fallthroughs = make([]bool, lCases)
	}

	for i, cConf := range cases {
		w, err := cConf.FieldOutput(soFieldCasesOutput)
		if err != nil {
			return nil, err
		}
		o.outputs[i] = interop.UnwrapOwnedOutput(w)

		oMgr := mgr.IntoPath("switch", strconv.Itoa(i), "output")
		if retryUntilSuccess {
			if o.outputs[i], err = RetryOutputIndefinitely(oMgr, o.outputs[i]); err != nil {
				return nil, fmt.Errorf("failed to create case '%v' output: %v", i, err)
			}
		}

		if checkStr, _ := cConf.FieldString(soFieldCasesCheck); checkStr != "" {
			if o.checks[i], err = mgr.BloblEnvironment().NewMapping(checkStr); err != nil {
				return nil, fmt.Errorf("failed to parse case '%v' check mapping: %v", i, err)
			}
		}
		if o.continues[i], err = cConf.FieldBool(soFieldCasesContinue); err != nil {
			return nil, err
		}
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
		ctx, done := o.shutSig.HardStopCtx(context.Background())
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
				var bErr *batch.Error
				if errors.As(err, &bErr) {
					bErr.WalkPartsBySource(group, sourceMessage, func(i int, p *message.Part, e error) bool {
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
		case <-o.shutSig.HardStopChan():
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
			case <-o.shutSig.HardStopChan():
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
		o.shutSig.TriggerHasStopped()
	}()

	shutCtx, done := o.shutSig.HardStopCtx(context.Background())
	defer done()

	for !o.shutSig.IsHardStopSignalled() {
		var ts message.Transaction
		var open bool

		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.shutSig.HardStopChan():
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
						o.logger.Error("Failed to test case %v: %v\n", j, err)
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
				o.logger.Error("Message failed to match against at least one output check with strict mode enabled, it will be nacked and/or re-processed")
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
	o.shutSig.TriggerHardStop()
}

func (o *switchOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-o.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
