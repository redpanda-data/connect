package output

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"golang.org/x/sync/errgroup"
)

//------------------------------------------------------------------------------

// SwitchConfigOutput contains configuration fields per output of a switch type.
type SwitchConfigOutput struct {
	Condition   condition.Config `json:"condition" yaml:"condition"`
	Fallthrough bool             `json:"fallthrough" yaml:"fallthrough"`
	Output      Config           `json:"output" yaml:"output"`
}

// NewSwitchConfigOutput creates a new switch output config with default values.
func NewSwitchConfigOutput() SwitchConfigOutput {
	cond := condition.NewConfig()
	cond.Type = condition.TypeStatic
	cond.Static = true

	return SwitchConfigOutput{
		Condition:   cond,
		Fallthrough: false,
		Output:      NewConfig(),
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchConfigOutput) UnmarshalJSON(bytes []byte) error {
	type confAlias SwitchConfigOutput
	aliased := confAlias(NewSwitchConfigOutput())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*s = SwitchConfigOutput(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchConfigOutput) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias SwitchConfigOutput
	aliased := confAlias(NewSwitchConfigOutput())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*s = SwitchConfigOutput(aliased)
	return nil
}

//------------------------------------------------------------------------------

func (o *Switch) loopDeprecated() {
	var (
		wg         = sync.WaitGroup{}
		mMsgRcvd   = o.stats.GetCounter("switch.messages.received")
		mMsgSnt    = o.stats.GetCounter("switch.messages.sent")
		mOutputErr = o.stats.GetCounter("switch.output.error")
	)

	defer func() {
		wg.Wait()
		for i, output := range o.outputs {
			output.CloseAsync()
			close(o.outputTsChans[i])
		}
		for _, output := range o.outputs {
			if err := output.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = output.WaitForClose(time.Second)
				}
			}
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
			mMsgRcvd.Incr(1)

			var outputTargets []int
			for i, oCond := range o.conditions {
				if oCond.Check(ts.Payload) {
					outputTargets = append(outputTargets, i)
					if !o.fallthroughs[i] {
						break
					}
				}
			}

			if o.strictMode && len(outputTargets) == 0 {
				select {
				case ts.ResponseChan <- response.NewError(ErrSwitchNoConditionMet):
				case <-o.ctx.Done():
					return
				}
				continue
			}

			var owg errgroup.Group
			for _, target := range outputTargets {
				msgCopy, i := ts.Payload.Copy(), target
				owg.Go(func() error {
					throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
					resChan := make(chan types.Response)

					// Try until success or shutdown.
					for {
						select {
						case o.outputTsChans[i] <- types.NewTransaction(msgCopy, resChan):
						case <-o.ctx.Done():
							return types.ErrTypeClosed
						}
						select {
						case res := <-resChan:
							if res.Error() != nil {
								if o.retryUntilSuccess {
									o.logger.Errorf("Failed to dispatch switch message: %v\n", res.Error())
									mOutputErr.Incr(1)
									if !throt.Retry() {
										return types.ErrTypeClosed
									}
								} else {
									return res.Error()
								}
							} else {
								mMsgSnt.Incr(1)
								return nil
							}
						case <-o.ctx.Done():
							return types.ErrTypeClosed
						}
					}
				})
			}

			var oResponse types.Response = response.NewAck()
			if resErr := owg.Wait(); resErr != nil {
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

//------------------------------------------------------------------------------
