package serverless

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Handler provides a mechanism for controlling the lifetime of a serverless
// handler runtime of Redpanda Connect.
type Handler struct {
	prodFn service.MessageHandlerFunc
	strm   *service.Stream
}

// NewHandler creates a new serverless stream handler, where the provided config
// is used in order to determine the behaviour of the pipeline.
func NewHandler(confYAML string) (*Handler, error) {
	env := service.GlobalEnvironment()
	schema := env.FullConfigSchema("", "")
	schema.SetFieldDefault(map[string]any{
		"none": map[string]any{},
	}, "metrics")
	schema.SetFieldDefault("json", "logger", "format")
	schema.SetFieldDefault(map[string]any{
		"inproc": "____ignored",
	}, "input")
	schema.SetFieldDefault(map[string]any{
		"switch": map[string]any{
			"retry_until_success": false,
			"cases": []any{
				map[string]any{
					"check": "errored()",
					"output": map[string]any{
						"reject": "processing failed due to: ${! error() }",
					},
				},
				map[string]any{
					"output": map[string]any{
						"sync_response": map[string]any{},
					},
				},
			},
		},
	}, "output")

	strmBuilder := env.NewStreamBuilder()
	strmBuilder.SetSchema(schema)

	if err := strmBuilder.SetYAML(confYAML); err != nil {
		return nil, err
	}

	prod, err := strmBuilder.AddProducerFunc()
	if err != nil {
		return nil, err
	}

	strm, err := strmBuilder.Build()
	if err != nil {
		return nil, err
	}

	go func() {
		_ = strm.Run(context.Background())
	}()

	return &Handler{
		prodFn: prod,
		strm:   strm,
	}, nil
}

// Close shuts down the underlying pipeline.
func (h *Handler) Close(ctx context.Context) error {
	return h.strm.Stop(ctx)
}

// Handle is a request/response func that injects a payload into the underlying
// Benthos pipeline and returns a result.
func (h *Handler) Handle(ctx context.Context, v any) (any, error) {
	msg := service.NewMessage(nil)
	msg.SetStructured(v)

	msg, store := msg.WithSyncResponseStore()

	if err := h.prodFn(ctx, msg); err != nil {
		return nil, err
	}

	resultBatches := store.Read()

	anyResults := make([][]any, len(resultBatches))
	for i, batch := range resultBatches {
		batchResults := make([]any, len(batch))
		for j, p := range batch {
			var merr error
			if batchResults[j], merr = p.AsStructured(); merr != nil {
				return nil, fmt.Errorf("failed to process result batch '%v': failed to marshal json response: %v", i, merr)
			}
		}
		anyResults[i] = batchResults
	}

	if len(anyResults) == 1 {
		if len(anyResults[0]) == 1 {
			return anyResults[0][0], nil
		}
		return anyResults[0], nil
	}

	genBatchOfBatches := make([]any, len(anyResults))
	for i, b := range anyResults {
		genBatchOfBatches[i] = b
	}
	return genBatchOfBatches, nil
}
