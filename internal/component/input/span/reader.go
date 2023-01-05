package span

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

// ExtractTracingSpanMappingDocs returns a docs spec for a mapping field.
var ExtractTracingSpanMappingDocs = docs.FieldBloblang(
	"extract_tracing_map", "EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) that attempts to extract an object containing tracing propagation information, which will then be used as the root tracing span for the message. The specification of the extracted fields must match the format used by the service wide tracer.",
	`root = meta()`,
	`root = this.meta.span`,
).AtVersion("3.45.0").Advanced()

// Reader wraps an async reader with a mechanism for extracting tracing
// spans from the consumed message using a Bloblang mapping.
type Reader struct {
	inputName string
	mgr       component.Observability

	mapping *mapping.Executor
	rdr     input.Async
}

// NewReader wraps an async reader with a mechanism for extracting tracing
// spans from the consumed message using a Bloblang mapping.
func NewReader(inputName, mapping string, rdr input.Async, mgr bundle.NewManagement) (input.Async, error) {
	exe, err := mgr.BloblEnvironment().NewMapping(mapping)
	if err != nil {
		return nil, err
	}
	return &Reader{inputName: inputName, mgr: mgr, mapping: exe, rdr: rdr}, nil
}

// Connect attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (s *Reader) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

// ReadBatch attempts to read a new message from the source. If
// successful a message is returned along with a function used to
// acknowledge receipt of the returned message. It's safe to process the
// returned message and read the next message asynchronously.
func (s *Reader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	m, afn, err := s.rdr.ReadBatch(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := s.mapping.MapPart(0, m)
	if err != nil {
		s.mgr.Logger().Errorf("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	structured, err := spanPart.AsStructured()
	if err != nil {
		s.mgr.Logger().Errorf("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	spanMap, ok := structured.(map[string]any)
	if !ok {
		s.mgr.Logger().Errorf("Mapping failed for tracing span, expected an object, got: %T", structured)
		return m, afn, nil
	}

	if err := tracing.InitSpansFromParentTextMap(s.mgr.Tracer(), "input_"+s.inputName, spanMap, m); err != nil {
		s.mgr.Logger().Errorf("Extraction of parent tracing span failed: %v", err)
	}
	return m, afn, nil
}

// Close triggers the shut down of this component and blocks until completion or
// context cancellation.
func (s *Reader) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}
