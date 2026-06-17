// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package confluent

import (
	"context"
	"fmt"

	franz_sr "github.com/twmb/franz-go/pkg/sr"
	"github.com/xeipuuv/gojsonschema"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func resolveJSONSchema(ctx context.Context, client *sr.Client, schema franz_sr.Schema) (*gojsonschema.Schema, error) {
	sl := gojsonschema.NewSchemaLoader()

	if len(schema.References) == 0 {
		if err := sl.AddSchemas(); err != nil {
			return nil, fmt.Errorf("parsing root schema: %w", err)
		}

		return sl.Compile(gojsonschema.NewStringLoader(schema.Schema))
	}

	if err := client.WalkReferences(ctx, schema.References, func(_ context.Context, _ string, schema franz_sr.Schema) error {
		return sl.AddSchemas(gojsonschema.NewStringLoader(schema.Schema))
	}); err != nil {
		return nil, err
	}

	return sl.Compile(gojsonschema.NewStringLoader(schema.Schema))
}

func (s *schemaRegistryEncoder) getJSONEncoder(ctx context.Context, schema franz_sr.Schema) (schemaEncoder, error) {
	// Encoding never coerces: the message is validated and passed through as-is.
	return getJSONTranscoder(ctx, s.client, schema, false)
}

func (s *schemaRegistryDecoder) getJSONDecoder(ctx context.Context, schema franz_sr.Schema) (schemaDecoder, error) {
	return getJSONTranscoder(ctx, s.client, schema, s.cfg.json.coerceData)
}

func getJSONTranscoder(ctx context.Context, cl *sr.Client, schema franz_sr.Schema, coerce bool) (func(m *service.Message) error, error) {
	sch, err := resolveJSONSchema(ctx, cl, schema)
	if err != nil {
		return nil, err
	}

	// When coercion is disabled we only need to verify that the message is
	// valid, since the input format benthos uses (json) is the same as the
	// output format and the message is left unchanged.
	var coercer *jsonSchemaCoercer
	if coerce {
		if coercer, err = buildJSONSchemaCoercer(ctx, cl, schema); err != nil {
			return nil, err
		}
	}

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

		// When enabled, rebuild the message using Go types that match the
		// declared schema (e.g. integer -> int64) so downstream components
		// infer the correct types. This makes decoding a transforming rather
		// than validate-only operation.
		if coercer != nil {
			out, err := coercer.coerceMessage(b)
			if err != nil {
				return fmt.Errorf("coercing message to schema: %w", err)
			}
			m.SetStructuredMut(out)
		}

		return nil
	}, nil
}
