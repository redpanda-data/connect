package service

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

const (
	etsField = "extract_tracing_map"
)

// NewExtractTracingSpanMappingDocs returns a config field for mapping messages
// in order to extract distributed tracing information.
func NewExtractTracingSpanMappingField() *ConfigField {
	return NewBloblangField(etsField).
		Description("EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) that attempts to extract an object containing tracing propagation information, which will then be used as the root tracing span for the message. The specification of the extracted fields must match the format used by the service wide tracer.").
		Examples(`root = @`, `root = this.meta.span`).
		Version("3.45.0").
		Optional().
		Advanced()
}

// WrapBatchInputExtractTracingSpanMapping wraps a BatchInput with a mechanism
// for extracting tracing spans using a bloblang mapping.
func (p *ParsedConfig) WrapBatchInputExtractTracingSpanMapping(inputName string, i BatchInput) (
	BatchInput, error) {
	if str, _ := p.FieldString(etsField); str == "" {
		return i, nil
	}
	exe, err := p.FieldBloblang(etsField)
	if err != nil {
		return nil, err
	}
	return &spanInjectBatchInput{inputName: inputName, mgr: p.mgr, mapping: exe, rdr: i}, nil
}

// WrapInputExtractTracingSpanMapping wraps a Input with a mechanism for
// extracting tracing spans from the consumed message using a Bloblang mapping.
func (p *ParsedConfig) WrapInputExtractTracingSpanMapping(inputName string, i Input) (Input, error) {
	if str, _ := p.FieldString(etsField); str == "" {
		return i, nil
	}
	exe, err := p.FieldBloblang(etsField)
	if err != nil {
		return nil, err
	}
	return &spanInjectInput{inputName: inputName, mgr: p.mgr, mapping: exe, rdr: i}, nil
}

func getPropMapCarrier(spanPart *Message) (propagation.MapCarrier, error) {
	structured, err := spanPart.AsStructured()
	if err != nil {
		return nil, err
	}

	spanMap, ok := structured.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected an object, got: %T", structured)
	}

	c := propagation.MapCarrier{}
	for k, v := range spanMap {
		if vStr, ok := v.(string); ok {
			c[strings.ToLower(k)] = vStr
		}
	}
	return c, nil
}

// spanInjectBatchInput wraps a BatchInput with a mechanism for
// extracting tracing spans from the consumed message using a Bloblang mapping.
type spanInjectBatchInput struct {
	inputName string
	mgr       bundle.NewManagement

	mapping *bloblang.Executor
	rdr     BatchInput
}

func (s *spanInjectBatchInput) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

func (s *spanInjectBatchInput) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	m, afn, err := s.rdr.ReadBatch(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := m.BloblangQuery(0, s.mapping)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	c, err := getPropMapCarrier(spanPart)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	prov := s.mgr.Tracer()
	operationName := "input_" + s.inputName

	textProp := otel.GetTextMapPropagator()
	for i, p := range m {
		ctx := textProp.Extract(p.Context(), c)
		pCtx, _ := prov.Tracer("benthos").Start(ctx, operationName)
		m[i] = p.WithContext(pCtx)
	}
	return m, afn, nil
}

func (s *spanInjectBatchInput) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}

// spanInjectInput wraps a Input with a mechanism for extracting tracing
// spans from the consumed message using a Bloblang mapping.
type spanInjectInput struct {
	inputName string
	mgr       bundle.NewManagement

	mapping *bloblang.Executor
	rdr     Input
}

func (s *spanInjectInput) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

func (s *spanInjectInput) Read(ctx context.Context) (*Message, AckFunc, error) {
	m, afn, err := s.rdr.Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := m.BloblangQuery(s.mapping)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	c, err := getPropMapCarrier(spanPart)
	if err != nil {
		s.mgr.Logger().Error("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	prov := s.mgr.Tracer()
	operationName := "input_" + s.inputName

	textProp := otel.GetTextMapPropagator()

	pCtx, _ := prov.Tracer("benthos").Start(textProp.Extract(m.Context(), c), operationName)
	m = m.WithContext(pCtx)

	return m, afn, nil
}

func (s *spanInjectInput) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}
