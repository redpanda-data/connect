package service

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

const (
	itsField = "inject_tracing_map"
)

// NewInjectTracingSpanMappingField returns a field spec describing an inject
// tracing span mapping.
func NewInjectTracingSpanMappingField() *ConfigField {
	return NewBloblangField(itsField).
		Description("EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) used to inject an object containing tracing propagation information into outbound messages. The specification of the injected fields will match the format used by the service wide tracer.").
		Examples(
			`meta = @.merge(this)`,
			`root.meta.span = this`,
		).
		Version("3.45.0").
		Optional().
		Advanced()
}

// WrapBatchOutputExtractTracingSpanMapping wraps a BatchOutput with a mechanism
// for extracting tracing spans from the consumed message and merging them into
// the written result using a Bloblang mapping.
func (p *ParsedConfig) WrapBatchOutputExtractTracingSpanMapping(outputName string, o BatchOutput) (BatchOutput, error) {
	if str, _ := p.FieldString(itsField); str == "" {
		return o, nil
	}
	exe, err := p.FieldBloblang(itsField)
	if err != nil {
		return nil, err
	}
	return &spanExtractBatchOutput{outputName: outputName, mgr: p.mgr, mapping: exe, wtr: o}, nil
}

// WrapOutputExtractTracingSpanMapping wraps a Output with a mechanism for
// extracting tracing spans from the consumed message and merging them into the
// written result using a Bloblang mapping.
func (p *ParsedConfig) WrapOutputExtractTracingSpanMapping(outputName string, o Output) (Output, error) {
	if str, _ := p.FieldString(itsField); str == "" {
		return o, nil
	}
	exe, err := p.FieldBloblang(itsField)
	if err != nil {
		return nil, err
	}
	return &spanExtractOutput{outputName: outputName, mgr: p.mgr, mapping: exe, wtr: o}, nil
}

type spanExtractBatchOutput struct {
	outputName string
	mgr        bundle.NewManagement

	mapping *bloblang.Executor
	wtr     BatchOutput
}

func (s *spanExtractBatchOutput) Connect(ctx context.Context) error {
	return s.wtr.Connect(ctx)
}

func (s *spanExtractBatchOutput) WriteBatch(ctx context.Context, batch MessageBatch) error {
	for i := 0; i < len(batch); i++ {
		span := tracing.GetSpanFromContext(batch[i].Context())
		spanMapGeneric, err := span.TextMap()
		if err != nil {
			s.mgr.Logger().Warn("Failed to inject span: %v", err)
			continue
		}

		spanMsg := NewMessage(nil)
		spanMsg.SetStructuredMut(spanMapGeneric)

		if tmpRes, err := batch[i].BloblangMutateFrom(s.mapping, spanMsg); err != nil {
			s.mgr.Logger().Warn("Failed to inject span: %v", err)
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
	mgr        bundle.NewManagement

	mapping *bloblang.Executor
	wtr     Output
}

func (s *spanExtractOutput) Connect(ctx context.Context) error {
	return s.wtr.Connect(ctx)
}

func (s *spanExtractOutput) Write(ctx context.Context, msg *Message) error {
	span := tracing.GetSpanFromContext(msg.Context())
	spanMapGeneric, err := span.TextMap()
	if err != nil {
		s.mgr.Logger().Warn("Failed to inject span: %v", err)
		return s.wtr.Write(ctx, msg)
	}

	spanMsg := NewMessage(nil)
	spanMsg.SetStructuredMut(spanMapGeneric)

	if tmpRes, err := msg.BloblangMutateFrom(s.mapping, spanMsg); err != nil {
		s.mgr.Logger().Warn("Failed to inject span: %v", err)
	} else if tmpRes != nil {
		msg = tmpRes
	}
	return s.wtr.Write(ctx, msg)
}

func (s *spanExtractOutput) Close(ctx context.Context) error {
	return s.wtr.Close(ctx)
}
