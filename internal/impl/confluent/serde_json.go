package confluent

import (
	"context"
	"fmt"

	"github.com/xeipuuv/gojsonschema"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func resolveJSONSchema(ctx context.Context, client *schemaRegistryClient, info SchemaInfo) (*gojsonschema.Schema, error) {
	sl := gojsonschema.NewSchemaLoader()

	if len(info.References) == 0 {
		if err := sl.AddSchemas(); err != nil {
			return nil, fmt.Errorf("failed to parse root schema: %w", err)
		}

		return sl.Compile(gojsonschema.NewStringLoader(info.Schema))
	}

	if err := client.WalkReferences(ctx, info.References, func(ctx context.Context, name string, info SchemaInfo) error {
		return sl.AddSchemas(gojsonschema.NewStringLoader(info.Schema))
	}); err != nil {
		return nil, err
	}

	return sl.Compile(gojsonschema.NewStringLoader(info.Schema))
}

func (s *schemaRegistryEncoder) getJSONEncoder(ctx context.Context, info SchemaInfo) (schemaEncoder, error) {
	return getJSONTranscoder(ctx, s.client, info)
}

func (s *schemaRegistryDecoder) getJSONDecoder(ctx context.Context, info SchemaInfo) (schemaDecoder, error) {
	return getJSONTranscoder(ctx, s.client, info)
}

func getJSONTranscoder(ctx context.Context, cl *schemaRegistryClient, info SchemaInfo) (func(m *service.Message) error, error) {
	sch, err := resolveJSONSchema(ctx, cl, info)
	if err != nil {
		return nil, err
	}

	// -- we only need to verify if the message is valid since the input format which benthos uses (json) is the same
	// -- as the output format
	return func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		// -- verify the json message against the schema
		res, err := sch.Validate(gojsonschema.NewBytesLoader(b))
		if err != nil {
			return err
		}

		if !res.Valid() {
			return fmt.Errorf("json message does not conform to schema: %v", res.Errors())
		}

		return nil
	}, nil
}
