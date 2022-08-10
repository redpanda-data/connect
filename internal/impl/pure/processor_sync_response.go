package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		return &syncResponseProc{log: mgr.Logger()}, nil
	}, docs.ComponentSpec{
		Name: "sync_response",
		Categories: []string{
			"Utility",
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
		Config: docs.FieldObject("", "").HasDefault(struct{}{}),
	})
	if err != nil {
		panic(err)
	}
}

type syncResponseProc struct {
	log log.Modular
}

func (s *syncResponseProc) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	if err := transaction.SetAsResponse(msg); err != nil {
		s.log.Debugf("Failed to store message as a sync response: %v\n", err)
	}
	return []message.Batch{msg}, nil
}

func (s *syncResponseProc) Close(context.Context) error {
	return nil
}
