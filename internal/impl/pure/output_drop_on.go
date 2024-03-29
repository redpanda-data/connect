package pure

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	dooFieldError         = "error"
	dooFieldErrorPatterns = "error_patterns"
	dooFieldBackPressure  = "back_pressure"
	dooFieldOutput        = "output"
)

func dropOnOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary(`Attempts to write messages to a child output and if the write fails for one of a list of configurable reasons the message is dropped (acked) instead of being reattempted (or nacked).`).
		Description(`Regular Benthos outputs will apply back pressure when downstream services aren't accessible, and Benthos retries (or nacks) all messages that fail to be delivered. However, in some circumstances, or for certain output types, we instead might want to relax these mechanisms, which is when this output becomes useful.`).
		Example(
			"Dropping failed HTTP requests",
			"In this example we have a fan_out broker, where we guarantee delivery to our Kafka output, but drop messages if they fail our secondary HTTP client output.",
			`
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka:
          addresses: [ foobar:6379 ]
          topic: foo
      - drop_on:
          error: true
          output:
            http_client:
              url: http://example.com/foo/messages
              verb: POST
`,
		).
		Example(
			"Dropping from outputs that cannot connect",
			"Most outputs that attempt to establish and long-lived connection will apply back-pressure when the connection is lost. The following example has a websocket output where if it takes longer than 10 seconds to establish a connection, or recover a lost one, pending messages are dropped.",
			`
output:
  drop_on:
    back_pressure: 10s
    output:
      websocket:
        url: ws://example.com/foo/messages
`,
		).
		Fields(
			service.NewBoolField(dooFieldError).
				Description("Whether messages should be dropped when the child output returns an error of any type. For example, this could be when an `http_client` output gets a 4XX response code. In order to instead drop only on specific error patterns use the `error_matches` field instead.").
				Default(false),
			service.NewStringListField(dooFieldErrorPatterns).
				Description("A list of regular expressions (re2) where if the child output returns an error that matches any part of any of these patterns the message will be dropped.").
				Optional().
				Version("4.27.0").
				Examples([]any{
					"and that was really bad$",
				}, []any{
					"roughly [0-9]+ issues occurred",
				}),
			service.NewDurationField(dooFieldBackPressure).
				Description("An optional duration string that determines the maximum length of time to wait for a given message to be accepted by the child output before the message should be dropped instead. The most common reason for an output to block is when waiting for a lost connection to be re-established. Once a message has been dropped due to back pressure all subsequent messages are dropped immediately until the output is ready to process them again. Note that if `error` is set to `false` and this field is specified then messages dropped due to back pressure will return an error response (are nacked or reattempted).").
				Examples("30s", "1m").
				Optional(),
			service.NewOutputField(dooFieldOutput).
				Description("A child output to wrap with this drop mechanism."),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"drop_on", dropOnOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1
			var s output.Streamed
			if s, err = newDropOnWriter(conf, interop.UnwrapManagement(mgr).Logger()); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(s)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type dropOnWriter struct {
	log log.Modular

	onError        bool
	onErrorMatches []*regexp.Regexp
	onBackpressure time.Duration
	wrapped        output.Streamed

	transactionsIn  <-chan message.Transaction
	transactionsOut chan message.Transaction

	shutSig *shutdown.Signaller
}

func newDropOnWriter(conf *service.ParsedConfig, log log.Modular) (*dropOnWriter, error) {
	onError, err := conf.FieldBool(dooFieldError)
	if err != nil {
		return nil, err
	}

	var onErrMatchesPatterns []*regexp.Regexp
	if onErrMatches, _ := conf.FieldStringList(dooFieldErrorPatterns); len(onErrMatches) > 0 {
		if onError {
			return nil, fmt.Errorf("field '%v' is ineffective when '%v' is set to `true`", dooFieldErrorPatterns, dooFieldError)
		}
		for i, str := range onErrMatches {
			tmp, err := regexp.Compile(str)
			if err != nil {
				return nil, fmt.Errorf("error pattern %v failed to compile: %w", i, err)
			}
			onErrMatchesPatterns = append(onErrMatchesPatterns, tmp)
		}
	}

	var backPressure time.Duration
	if bpStr, _ := conf.FieldString(dooFieldBackPressure); bpStr != "" {
		var err error
		if backPressure, err = time.ParseDuration(bpStr); err != nil {
			return nil, fmt.Errorf("failed to parse back_pressure duration: %w", err)
		}
	}

	pOut, err := conf.FieldOutput(dooFieldOutput)
	if err != nil {
		return nil, err
	}

	return &dropOnWriter{
		log:             log,
		wrapped:         interop.UnwrapOwnedOutput(pOut),
		transactionsOut: make(chan message.Transaction),

		onError:        onError,
		onErrorMatches: onErrMatchesPatterns,
		onBackpressure: backPressure,

		shutSig: shutdown.NewSignaller(),
	}, nil
}

func (d *dropOnWriter) loop() {
	cnCtx, cnDone := d.shutSig.CloseNowCtx(context.Background())
	defer func() {
		close(d.transactionsOut)

		d.wrapped.TriggerCloseNow()
		_ = d.wrapped.WaitForClose(context.Background())

		d.shutSig.ShutdownComplete()
		cnDone()
	}()

	resChan := make(chan error)

	var gotBackPressure bool
	for {
		var ts message.Transaction
		var open bool
		select {
		case ts, open = <-d.transactionsIn:
			if !open {
				return
			}
		case <-d.shutSig.CloseNowChan():
			return
		}

		var res error
		if d.onBackpressure > 0 {
			if !func() bool {
				// Use a ticker here and call Stop explicitly.
				ticker := time.NewTicker(d.onBackpressure)
				defer ticker.Stop()

				if gotBackPressure {
					select {
					case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
						gotBackPressure = false
					default:
					}
				} else {
					select {
					case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
					case <-ticker.C:
						gotBackPressure = true
					case <-d.shutSig.CloseNowChan():
						return false
					}
				}
				if !gotBackPressure {
					select {
					case res = <-resChan:
					case <-ticker.C:
						gotBackPressure = true
						go func() {
							// We must pull the response that we're due, since
							// the component isn't being shut down.
							<-resChan
						}()
					case <-d.shutSig.CloseNowChan():
						return false
					}
				}
				if gotBackPressure {
					d.log.Warn("Message dropped due to back pressure.")
					if d.onError {
						res = nil
					} else {
						res = fmt.Errorf("experienced back pressure beyond: %v", d.onBackpressure)
					}
				}
				return true
			}() {
				return
			}
		} else {
			// Push data as usual, if the output blocks due to a disconnect then
			// we wait as long as it takes.
			select {
			case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
			case <-d.shutSig.CloseNowChan():
				return
			}
			select {
			case res = <-resChan:
			case <-d.shutSig.CloseNowChan():
				return
			}
		}

		if res != nil && d.onError {
			d.log.Warn("Message dropped due to: %v", res)
			res = nil
		}

		if res != nil && len(d.onErrorMatches) > 0 {
			errStr := res.Error()
			for i, m := range d.onErrorMatches {
				if m.MatchString(errStr) {
					d.log.Warn("Message dropped due to error matching pattern %v: %v", i, res)
					res = nil
					break
				}
			}
		}

		if err := ts.Ack(cnCtx, res); err != nil && cnCtx.Err() != nil {
			return
		}
	}
}

func (d *dropOnWriter) Consume(ts <-chan message.Transaction) error {
	if d.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	if err := d.wrapped.Consume(d.transactionsOut); err != nil {
		return err
	}
	d.transactionsIn = ts
	go d.loop()
	return nil
}

func (d *dropOnWriter) Connected() bool {
	return d.wrapped.Connected()
}

func (d *dropOnWriter) TriggerCloseNow() {
	d.shutSig.CloseNow()
}

func (d *dropOnWriter) WaitForClose(ctx context.Context) error {
	select {
	case <-d.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
