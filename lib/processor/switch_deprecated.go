package processor

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

type switchCaseDeprecated struct {
	condition   types.Condition
	processors  []types.Processor
	fallThrough bool
}

type switchDeprecated struct {
	cases []switchCaseDeprecated
	log   log.Modular

	mCount     metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

func newSwitchDeprecated(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var cases []switchCaseDeprecated
	for i, caseConf := range conf.Switch {
		prefix := strconv.Itoa(i)

		var err error
		var cond types.Condition
		var procs []types.Processor

		cMgr, cLog, cStats := interop.LabelChild(prefix+".condition", mgr, log, stats)
		if cond, err = condition.New(caseConf.Condition, cMgr, cLog, cStats); err != nil {
			return nil, fmt.Errorf("case [%v] condition: %w", i, err)
		}

		for j, procConf := range caseConf.Processors {
			pMgr, pLog, pStats := interop.LabelChild(prefix+"."+strconv.Itoa(j), mgr, log, stats)
			var proc types.Processor
			if proc, err = New(procConf, pMgr, pLog, pStats); err != nil {
				return nil, fmt.Errorf("case [%v] processor [%v]: %w", i, j, err)
			}
			procs = append(procs, proc)
		}

		cases = append(cases, switchCaseDeprecated{
			condition:   cond,
			processors:  procs,
			fallThrough: caseConf.Fallthrough,
		})
	}
	return &switchDeprecated{
		cases: cases,
		log:   log,

		mCount:     stats.GetCounter("count"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

func (s *switchDeprecated) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	s.mCount.Incr(1)

	var procs []types.Processor
	fellthrough := false

	spans := tracing.CreateChildSpans(TypeSwitch, msg)

	for i, switchCase := range s.cases {
		if !fellthrough && !switchCase.condition.Check(msg) {
			continue
		}
		procs = append(procs, switchCase.processors...)
		for _, s := range spans {
			s.LogFields(
				olog.String("event", "case_match"),
				olog.String("value", strconv.Itoa(i)),
			)
		}
		if fellthrough = switchCase.fallThrough; !fellthrough {
			break
		}
	}

	for _, s := range spans {
		s.Finish()
	}

	msgs, res = ExecuteAll(procs, msg)

	s.mBatchSent.Incr(int64(len(msgs)))
	totalParts := 0
	for _, msg := range msgs {
		totalParts += msg.Len()
	}
	s.mSent.Incr(int64(totalParts))
	return
}

func (s *switchDeprecated) CloseAsync() {
	for _, s := range s.cases {
		for _, proc := range s.processors {
			proc.CloseAsync()
		}
	}
}

func (s *switchDeprecated) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, s := range s.cases {
		for _, proc := range s.processors {
			if err := proc.WaitForClose(time.Until(stopBy)); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
