package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSyncResponse] = TypeSpec{
		constructor: NewSyncResponse,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Adds the payload in its current state as a synchronous response to the input
source, where it is dealt with according to that specific input type.`,
		Description: `
For most inputs this mechanism is ignored entirely, in which case the sync
response is dropped without penalty. It is therefore safe to use this processor
even when combining input types that might not have support for sync responses.
An example of an input able to utilise this is the ` + "`http_server`" + `.

For more information please read [Synchronous Responses](/docs/guides/sync_responses).`,
		config: docs.FieldComponent().HasType(docs.FieldTypeObject),
	}
}

//------------------------------------------------------------------------------

// SyncResponseConfig contains configuration fields for the SyncResponse
// processor.
type SyncResponseConfig struct{}

// NewSyncResponseConfig returns a SyncResponseConfig with default values.
func NewSyncResponseConfig() SyncResponseConfig {
	return SyncResponseConfig{}
}

//------------------------------------------------------------------------------

// SyncResponse is a processor that prints a log event each time it processes a message.
type SyncResponse struct {
	log log.Modular
}

// NewSyncResponse returns a SyncResponse processor.
func NewSyncResponse(
	conf Config, mgr types.Manager, logger log.Modular, stats metrics.Type,
) (Type, error) {
	s := &SyncResponse{
		log: logger,
	}
	return s, nil
}

//------------------------------------------------------------------------------

// ProcessMessage logs an event and returns the message unchanged.
func (s *SyncResponse) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	if err := roundtrip.SetAsResponse(msg); err != nil {
		s.log.Debugf("Failed to store message as a sync response: %v\n", err)
	}
	return []*message.Batch{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *SyncResponse) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *SyncResponse) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
