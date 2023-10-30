package span

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	itsField = "inject_tracing_map"
)

// InjectTracingSpanMappingDocs returns a field spec describing an inject
// tracing span mapping.
func InjectTracingSpanMappingDocs() *service.ConfigField {
	return service.NewBloblangField(itsField).
		Description("EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) used to inject an object containing tracing propagation information into outbound messages. The specification of the injected fields will match the format used by the service wide tracer.").
		Examples(
			`meta = @.merge(this)`,
			`root.meta.span = this`,
		).
		Version("3.45.0").
		Optional().
		Advanced()
}

// NewBatchOutput wraps a service.BatchOutput with a mechanism for extracting
// tracing spans from the consumed message and merging them into the written
// result using a Bloblang mapping.
func NewBatchOutput(outputName string, pConf *service.ParsedConfig, wtr service.BatchOutput, mgr *service.Resources) (service.BatchOutput, error) {
	if str, _ := pConf.FieldString(itsField); str == "" {
		return wtr, nil
	}
	exe, err := pConf.FieldBloblang(itsField)
	if err != nil {
		return nil, err
	}
	return &spanExtractBatchOutput{outputName: outputName, mgr: mgr, mapping: exe, wtr: wtr}, nil
}

// NewOutput wraps a service.Output with a mechanism for extracting tracing
// spans from the consumed message and merging them into the written result
// using a Bloblang mapping.
func NewOutput(outputName string, pConf *service.ParsedConfig, wtr service.Output, mgr *service.Resources) (service.Output, error) {
	if str, _ := pConf.FieldString(itsField); str == "" {
		return wtr, nil
	}
	exe, err := pConf.FieldBloblang(itsField)
	if err != nil {
		return nil, err
	}
	return &spanExtractOutput{outputName: outputName, mgr: mgr, mapping: exe, wtr: wtr}, nil
}

type spanExtractBatchOutput struct {
	outputName string
	mgr        *service.Resources

	mapping *bloblang.Executor
	wtr     service.BatchOutput
}

func (s *spanExtractBatchOutput) Connect(ctx context.Context) error {
	return s.wtr.Connect(ctx)
}

func (s *spanExtractBatchOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for i := 0; i < len(batch); i++ {
		span := tracing.GetSpanFromContext(batch[i].Context())
		spanMapGeneric, err := span.TextMap()
		if err != nil {
			s.mgr.Logger().Warnf("Failed to inject span: %v", err)
			continue
		}

		spanMsg := service.NewMessage(nil)
		spanMsg.SetStructuredMut(spanMapGeneric)

		if tmpRes, err := batch[i].BloblangMutateFrom(s.mapping, spanMsg); err != nil {
			s.mgr.Logger().Warnf("Failed to inject span: %v", err)
		} else if tmpRes != nil {
			batch[i] = tmpRes
		}
	}
	return s.wtr.WriteBatch(ctx, batch)
}

func (s *spanExtractBatchOutput) Close(ctx context.Context) error {
	return s.wtr.Close(ctx)
}

type spanExtractOutput struct {
	outputName string
	mgr        *service.Resources

	mapping *bloblang.Executor
	wtr     service.Output
}

func (s *spanExtractOutput) Connect(ctx context.Context) error {
	return s.wtr.Connect(ctx)
}

func (s *spanExtractOutput) Write(ctx context.Context, msg *service.Message) error {
	span := tracing.GetSpanFromContext(msg.Context())
	spanMapGeneric, err := span.TextMap()
	if err != nil {
		s.mgr.Logger().Warnf("Failed to inject span: %v", err)
		return s.wtr.Write(ctx, msg)
	}

	spanMsg := service.NewMessage(nil)
	spanMsg.SetStructuredMut(spanMapGeneric)

	if tmpRes, err := msg.BloblangMutateFrom(s.mapping, spanMsg); err != nil {
		s.mgr.Logger().Warnf("Failed to inject span: %v", err)
	} else if tmpRes != nil {
		msg = tmpRes
	}
	return s.wtr.Write(ctx, msg)
}

func (s *spanExtractOutput) Close(ctx context.Context) error {
	return s.wtr.Close(ctx)
}
