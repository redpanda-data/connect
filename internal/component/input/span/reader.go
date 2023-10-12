package span

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	etsField = "extract_tracing_map"
)

// ExtractTracingSpanMappingDocs returns a docs spec for a mapping field.
func ExtractTracingSpanMappingDocs() *service.ConfigField {
	return service.NewBloblangField(etsField).
		Description("EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) that attempts to extract an object containing tracing propagation information, which will then be used as the root tracing span for the message. The specification of the extracted fields must match the format used by the service wide tracer.").
		Examples(`root = @`, `root = this.meta.span`).
		Version("3.45.0").
		Optional().
		Advanced()
}

// NewBatchInput wraps a service.BatchInput with a mechanism for extracting
// tracing spans from the consumed message using a Bloblang mapping.
func NewBatchInput(inputName string, pConf *service.ParsedConfig, rdr service.BatchInput, mgr *service.Resources) (service.BatchInput, error) {
	if str, _ := pConf.FieldString(etsField); str == "" {
		return rdr, nil
	}
	exe, err := pConf.FieldBloblang(etsField)
	if err != nil {
		return nil, err
	}
	return &spanInjectBatchInput{inputName: inputName, mgr: mgr, mapping: exe, rdr: rdr}, nil
}

// NewInput wraps a service.Input with a mechanism for extracting tracing spans
// from the consumed message using a Bloblang mapping.
func NewInput(inputName string, pConf *service.ParsedConfig, rdr service.Input, mgr *service.Resources) (service.Input, error) {
	if str, _ := pConf.FieldString(etsField); str == "" {
		return rdr, nil
	}
	exe, err := pConf.FieldBloblang(etsField)
	if err != nil {
		return nil, err
	}
	return &spanInjectInput{inputName: inputName, mgr: mgr, mapping: exe, rdr: rdr}, nil
}

func getPropMapCarrier(spanPart *service.Message) (propagation.MapCarrier, error) {
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

// spanInjectBatchInput wraps a service.BatchInput with a mechanism for
// extracting tracing spans from the consumed message using a Bloblang mapping.
type spanInjectBatchInput struct {
	inputName string
	mgr       *service.Resources

	mapping *bloblang.Executor
	rdr     service.BatchInput
}

func (s *spanInjectBatchInput) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

func (s *spanInjectBatchInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	m, afn, err := s.rdr.ReadBatch(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := m.BloblangQuery(0, s.mapping)
	if err != nil {
		s.mgr.Logger().Errorf("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	c, err := getPropMapCarrier(spanPart)
	if err != nil {
		s.mgr.Logger().Errorf("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	prov := s.mgr.OtelTracer()
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

// spanInjectInput wraps a service.Input with a mechanism for extracting tracing
// spans from the consumed message using a Bloblang mapping.
type spanInjectInput struct {
	inputName string
	mgr       *service.Resources

	mapping *bloblang.Executor
	rdr     service.Input
}

func (s *spanInjectInput) Connect(ctx context.Context) error {
	return s.rdr.Connect(ctx)
}

func (s *spanInjectInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	m, afn, err := s.rdr.Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	spanPart, err := m.BloblangQuery(s.mapping)
	if err != nil {
		s.mgr.Logger().Errorf("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	c, err := getPropMapCarrier(spanPart)
	if err != nil {
		s.mgr.Logger().Errorf("Mapping failed for tracing span: %v", err)
		return m, afn, nil
	}

	prov := s.mgr.OtelTracer()
	operationName := "input_" + s.inputName

	textProp := otel.GetTextMapPropagator()

	pCtx, _ := prov.Tracer("benthos").Start(textProp.Extract(m.Context(), c), operationName)
	m = m.WithContext(pCtx)

	return m, afn, nil
}

func (s *spanInjectInput) Close(ctx context.Context) error {
	return s.rdr.Close(ctx)
}
