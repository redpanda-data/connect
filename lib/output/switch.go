package output

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	imessage "github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"github.com/Jeffail/gabs/v2"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrSwitchNoConditionMet is returned when a message does not match any
	// output conditions.
	ErrSwitchNoConditionMet = errors.New("no switch output conditions were met by message")
	// ErrSwitchNoCasesMatched is returned when a message does not match any
	// output cases.
	ErrSwitchNoCasesMatched = errors.New("no switch cases were matched by message")
	// ErrSwitchNoOutputs is returned when creating a Switch type with less than
	// 2 outputs.
	ErrSwitchNoOutputs = errors.New("attempting to create switch with fewer than 2 cases")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSwitch] = TypeSpec{
		constructor: fromSimpleConstructor(NewSwitch),
		Summary: `
The switch output type allows you to route messages to different outputs based on their contents.`,
		Description: `
Messages must successfully route to one or more outputs, otherwise this is considered an error and the message is reprocessed. In order to explicitly drop messages that do not match your cases add one final case with a [drop output](/docs/components/outputs/drop).`,
		config: docs.FieldComponent().WithChildren(
			docs.FieldCommon(
				"retry_until_success", `
If a selected output fails to send a message this field determines whether it is
reattempted indefinitely. If set to false the error is instead propagated back
to the input level.

If a message can be routed to >1 outputs it is usually best to set this to true
in order to avoid duplicate messages being routed to an output.`,
			),
			docs.FieldAdvanced(
				"strict_mode", `
This field determines whether an error should be reported if no condition is met.
If set to true, an error is propagated back to the input level. The default
behavior is false, which will drop the message.`,
			),
			docs.FieldAdvanced(
				"max_in_flight", "The maximum number of parallel message batches to have in flight at any given time. Note that if a child output has a higher `max_in_flight` then the switch output will automatically match it, therefore this value is the minimum `max_in_flight` to set in cases where the child values can't be inferred (such as when using resource outputs as children).",
			),
			docs.FieldCommon(
				"cases",
				"A list of switch cases, outlining outputs that can be routed to.",
				[]interface{}{
					map[string]interface{}{
						"check": `this.urls.contains("http://benthos.dev")`,
						"output": map[string]interface{}{
							"cache": map[string]interface{}{
								"target": "foo",
								"key":    "${!json(\"id\")}",
							},
						},
						"continue": true,
					},
					map[string]interface{}{
						"output": map[string]interface{}{
							"s3": map[string]interface{}{
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
				docs.FieldCommon(
					"output", "An [output](/docs/components/outputs/about/) for messages that pass the check to be routed to.",
				).HasDefault(map[string]interface{}{}).HasType(docs.FieldTypeOutput),
				docs.FieldAdvanced(
					"continue",
					"Indicates whether, if this case passes for a message, the next case should also be tested.",
				).HasDefault(false).HasType(docs.FieldTypeBool),
			),
		).Linter(func(ctx docs.LintContext, line, col int, value interface{}) []docs.Lint {
			gObj := gabs.Wrap(value)
			retry, exists := gObj.S("retry_until_success").Data().(bool)
			// TODO: V4 Is retry_until_success going to be false by default now?
			if exists && !retry {
				return nil
			}
			for _, cObj := range gObj.S("cases").Children() {
				typeStr, _ := cObj.S("output", "type").Data().(string)
				isReject := cObj.Exists("output", "reject")
				if typeStr == "reject" || isReject {
					return []docs.Lint{
						docs.NewLintError(line, "a `switch` output with a `reject` case output must have the field `switch.retry_until_success` set to `false` (defaults to `true`), otherwise the `reject` child output will result in infinite retries"),
					}
				}
			}
			return nil
		}),
		Categories: []Category{
			CategoryUtility,
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
            - bloblang: |
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
	}
}

//------------------------------------------------------------------------------

// SwitchConfig contains configuration fields for the Switch output type.
type SwitchConfig struct {
	RetryUntilSuccess bool               `json:"retry_until_success" yaml:"retry_until_success"`
	StrictMode        bool               `json:"strict_mode" yaml:"strict_mode"`
	MaxInFlight       int                `json:"max_in_flight" yaml:"max_in_flight"`
	Cases             []SwitchConfigCase `json:"cases" yaml:"cases"`
}

// NewSwitchConfig creates a new SwitchConfig with default values.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{
		RetryUntilSuccess: true,
		// TODO: V4 consider making this true by default.
		StrictMode:  false,
		MaxInFlight: 1,
		Cases:       []SwitchConfigCase{},
	}
}

// SwitchConfigCase contains configuration fields per output of a switch type.
type SwitchConfigCase struct {
	Check    string `json:"check" yaml:"check"`
	Continue bool   `json:"continue" yaml:"continue"`
	Output   Config `json:"output" yaml:"output"`
}

// NewSwitchConfigCase creates a new switch output config with default values.
func NewSwitchConfigCase() SwitchConfigCase {
	return SwitchConfigCase{
		Check:    "",
		Continue: false,
		Output:   NewConfig(),
	}
}

//------------------------------------------------------------------------------

// Switch is a broker that implements types.Consumer and broadcasts each message
// out to an array of outputs.
type Switch struct {
	logger     log.Modular
	stats      metrics.Type
	mMsgRcvd   metrics.StatCounter
	mMsgSnt    metrics.StatCounter
	mOutputErr metrics.StatCounter

	maxInFlight  int
	transactions <-chan types.Transaction

	retryUntilSuccess bool
	strictMode        bool
	outputTSChans     []chan types.Transaction
	outputs           []types.Output
	checks            []*mapping.Executor
	continues         []bool
	fallthroughs      []bool

	ctx        context.Context
	close      func()
	closedChan chan struct{}
}

// NewSwitch creates a new Switch type by providing outputs. Messages will be
// sent to a subset of outputs according to condition and fallthrough settings.
func NewSwitch(
	conf Config,
	mgr types.Manager,
	logger log.Modular,
	stats metrics.Type,
) (Type, error) {
	ctx, done := context.WithCancel(context.Background())
	o := &Switch{
		stats:             stats,
		logger:            logger,
		maxInFlight:       conf.Switch.MaxInFlight,
		transactions:      nil,
		retryUntilSuccess: conf.Switch.RetryUntilSuccess,
		strictMode:        conf.Switch.StrictMode,
		closedChan:        make(chan struct{}),
		ctx:               ctx,
		close:             done,
		mMsgRcvd:          stats.GetCounter("switch.messages.received"),
		mMsgSnt:           stats.GetCounter("switch.messages.sent"),
		mOutputErr:        stats.GetCounter("switch.output.error"),
	}

	lCases := len(conf.Switch.Cases)
	if lCases < 2 {
		return nil, ErrSwitchNoOutputs
	}
	if lCases > 0 {
		o.outputs = make([]types.Output, lCases)
		o.checks = make([]*mapping.Executor, lCases)
		o.continues = make([]bool, lCases)
		o.fallthroughs = make([]bool, lCases)
	}

	var err error
	for i, cConf := range conf.Switch.Cases {
		oMgr, oLog, oStats := interop.LabelChild(fmt.Sprintf("switch.%v.output", i), mgr, logger, stats)
		oStats = metrics.Combine(stats, oStats)
		if o.outputs[i], err = New(cConf.Output, oMgr, oLog, oStats); err != nil {
			return nil, fmt.Errorf("failed to create case '%v' output type '%v': %v", i, cConf.Output.Type, err)
		}
		if len(cConf.Check) > 0 {
			if o.checks[i], err = interop.NewBloblangMapping(mgr, cConf.Check); err != nil {
				return nil, fmt.Errorf("failed to parse case '%v' check mapping: %v", i, err)
			}
		}
		o.continues[i] = cConf.Continue
	}

	o.outputTSChans = make([]chan types.Transaction, len(o.outputs))
	for i := range o.outputTSChans {
		if mif, ok := output.GetMaxInFlight(o.outputs[i]); ok && mif > o.maxInFlight {
			o.maxInFlight = mif
		}
		o.outputTSChans[i] = make(chan types.Transaction)
		if err := o.outputs[i].Consume(o.outputTSChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *Switch) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (o *Switch) MaxInFlight() (int, bool) {
	return o.maxInFlight, true
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *Switch) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

func (o *Switch) dispatchRetryOnErr(outputTargets [][]types.Part) error {
	var owg errgroup.Group
	for target, parts := range outputTargets {
		if len(parts) == 0 {
			continue
		}
		msgCopy, i := message.New(nil), target
		msgCopy.SetAll(parts)
		owg.Go(func() error {
			throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
			resChan := make(chan types.Response)

			// Try until success or shutdown.
			for {
				select {
				case o.outputTSChans[i] <- types.NewTransaction(msgCopy, resChan):
				case <-o.ctx.Done():
					return types.ErrTypeClosed
				}
				select {
				case res := <-resChan:
					if res.Error() != nil {
						o.logger.Errorf("Failed to dispatch switch message: %v\n", res.Error())
						o.mOutputErr.Incr(1)
						if !throt.Retry() {
							return types.ErrTypeClosed
						}
					} else {
						o.mMsgSnt.Incr(1)
						return nil
					}
				case <-o.ctx.Done():
					return types.ErrTypeClosed
				}
			}
		})
	}
	return owg.Wait()
}

func (o *Switch) dispatchNoRetries(group *imessage.SortGroup, sourceMessage types.Message, outputTargets [][]types.Part) error {
	var wg sync.WaitGroup

	var setErr func(error)
	var setErrForPart func(types.Part, error)
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
		setErrForPart = func(part types.Part, err error) {
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

	for target, parts := range outputTargets {
		if len(parts) == 0 {
			continue
		}
		wg.Add(1)
		msgCopy, i := message.New(nil), target
		msgCopy.SetAll(parts)

		go func() {
			defer wg.Done()

			resChan := make(chan types.Response)
			select {
			case o.outputTSChans[i] <- types.NewTransaction(msgCopy, resChan):
			case <-o.ctx.Done():
				setErr(types.ErrTypeClosed)
				return
			}
			select {
			case res := <-resChan:
				if res.Error() != nil {
					o.mOutputErr.Incr(1)
					if bErr, ok := res.Error().(*batch.Error); ok {
						bErr.WalkParts(func(i int, p types.Part, e error) bool {
							if e != nil {
								setErrForPart(p, e)
							}
							return true
						})
					} else {
						msgCopy.Iter(func(i int, p types.Part) error {
							setErrForPart(p, res.Error())
							return nil
						})
					}
				} else {
					o.mMsgSnt.Incr(1)
				}
			case <-o.ctx.Done():
				setErr(types.ErrTypeClosed)
			}
		}()
	}

	wg.Wait()
	return getErr()
}

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *Switch) loop() {
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		for i, output := range o.outputs {
			output.CloseAsync()
			close(o.outputTSChans[i])
		}
		for _, output := range o.outputs {
			_ = output.WaitForClose(shutdown.MaximumShutdownWait())
		}
		close(o.closedChan)
	}()

	sendLoop := func() {
		defer wg.Done()
		for {
			var ts types.Transaction
			var open bool

			select {
			case ts, open = <-o.transactions:
				if !open {
					return
				}
			case <-o.ctx.Done():
				return
			}
			o.mMsgRcvd.Incr(1)

			group, trackedMsg := imessage.NewSortGroup(ts.Payload)

			outputTargets := make([][]types.Part, len(o.checks))
			if checksErr := trackedMsg.Iter(func(i int, p types.Part) error {
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
						outputTargets[j] = append(outputTargets[j], p.Copy())
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
				select {
				case ts.ResponseChan <- response.NewError(checksErr):
				case <-o.ctx.Done():
					return
				}
				continue
			}

			var resErr error
			if o.retryUntilSuccess {
				resErr = o.dispatchRetryOnErr(outputTargets)
			} else {
				resErr = o.dispatchNoRetries(group, trackedMsg, outputTargets)
			}

			var oResponse types.Response = response.NewAck()
			if resErr != nil {
				oResponse = response.NewError(resErr)
			}
			select {
			case ts.ResponseChan <- oResponse:
			case <-o.ctx.Done():
				return
			}
		}
	}

	// Max in flight
	for i := 0; i < o.maxInFlight; i++ {
		wg.Add(1)
		go sendLoop()
	}
}

// CloseAsync shuts down the Switch broker and stops processing requests.
func (o *Switch) CloseAsync() {
	o.close()
}

// WaitForClose blocks until the Switch broker has closed down.
func (o *Switch) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}
