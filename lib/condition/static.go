package condition

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeStatic] = TypeSpec{
		constructor: NewStatic,
		Description: `
Static is a condition that always resolves to the same static boolean value.`,
	}
}

//------------------------------------------------------------------------------

// Static is a condition that always returns a static boolean value.
type Static struct {
	value bool

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewStatic returns a Static condition.
func NewStatic(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Static{
		value: conf.Static,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (s *Static) Check(msg types.Message) bool {
	s.mCount.Incr(1)
	if s.value {
		s.mTrue.Incr(1)
	} else {
		s.mFalse.Incr(1)
	}
	return s.value
}

//------------------------------------------------------------------------------
