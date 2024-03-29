package pure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	spFieldDuration = "duration"
)

func init() {
	err := service.RegisterBatchProcessor("sleep", service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`Sleep for a period of time specified as a duration string for each message. This processor will interpolate functions within the `+"`duration`"+` field, you can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`).
		Field(service.NewInterpolatedStringField(spFieldDuration).
			Description("The duration of time to sleep for each execution.")),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			sleepStr, err := conf.FieldString(spFieldDuration)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newSleep(sleepStr, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("sleep", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type sleepProc struct {
	closeOnce   sync.Once
	closeChan   chan struct{}
	durationStr *field.Expression
	log         log.Modular
}

func newSleep(sleepStr string, mgr bundle.NewManagement) (*sleepProc, error) {
	durationStr, err := mgr.BloblEnvironment().NewField(sleepStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse duration expression: %v", err)
	}
	t := &sleepProc{
		closeChan:   make(chan struct{}),
		durationStr: durationStr,
		log:         mgr.Logger(),
	}
	return t, nil
}

func (s *sleepProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	for i := range msg {
		periodStr, err := s.durationStr.String(i, msg)
		if err != nil {
			s.log.Error("Period interpolation error: %v", err)
			continue
		}

		period, err := time.ParseDuration(periodStr)
		if err != nil {
			s.log.Error("Failed to parse duration: %v", err)
			continue
		}

		select {
		case <-time.After(period):
		case <-ctx.Context().Done():
			return nil, ctx.Context().Err()
		case <-s.closeChan:
			return nil, errors.New("processor stopped")
		}
	}
	return []message.Batch{msg}, nil
}

func (s *sleepProc) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
	return nil
}
