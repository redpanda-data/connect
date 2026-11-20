// Copyright 2026 Redpanda Data, Inc.
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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	franz_sr "github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/parquet"
	"github.com/redpanda-data/connect/v4/internal/impl/protobuf/common"
)

const (
	metricParquetProtobufDirect   = "parquet_encode_protobuf_direct_batches"
	metricParquetProtobufFallback = "parquet_encode_protobuf_fallback_batches"

	// directSchemaFetchTimeout bounds a schema lookup of the direct path. It
	// matches the lookup timeout of the schema_registry_decode processor.
	directSchemaFetchTimeout = 5 * time.Second
)

func parquetEncodeProtobufConfig() *service.ConfigSpec {
	return schemaRegistryDecoderConfig().
		Beta().
		Categories("Parsing", "Integration").
		Version("4.120.0").
		Summary("Decodes a batch of schema registry framed messages and encodes them as a single Parquet file.").
		Description(`
This processor is the combination of a `+"xref:components:processors/schema_registry_decode.adoc[`schema_registry_decode`]"+` processor followed by a `+"xref:components:processors/parquet_encode.adoc[`parquet_encode`]"+` processor. It accepts the fields of both, behaves like both run one after the other, and produces the same Parquet file.

The difference is how the work is done. When the messages of a batch are Protobuf and their schema maps onto the Parquet schema in a simple way, the Protobuf wire format is written straight into Parquet columns. This skips building an intermediate document for each message, which uses considerably less CPU and memory.

A batch takes this direct path only when all of the following hold. Otherwise, the batch is decoded and encoded exactly as the two separate processors would do it.

- `+"`protobuf.serialize_to_json`"+` is `+"`false`"+` and the Parquet schema is set with the `+"`schema`"+` field.
- Every Parquet column is a top level `+"`BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `UTF8` or `ENUM`"+` column, optionally repeated.
- The Protobuf message only has scalar, enum and repeated scalar fields, with no required fields, oneofs, maps or nested messages. Each field that has a column is of a type matching that column.
- Every message of the batch is plain canonical Protobuf: no unknown fields, no field written twice, no explicitly written default values and valid UTF-8 strings.

The first time a schema or a batch is found not to qualify, the reason is logged. The metrics `+"`"+metricParquetProtobufDirect+"`"+` and `+"`"+metricParquetProtobufFallback+"`"+` count the batches that took each path.

== Metadata

The resulting message carries the metadata of the first message of the batch, including the `+"`schema_id`"+` metadata added when decoding.
`).
		Fields(parquet.EncodeProcessorFields()...).
		LintRule(`root = if this.schema.or([]).length() == 0 && this.schema_metadata.or("") == "" { "either a schema or schema_metadata must be specified" }`).
		Example("Writing Protobuf topics as Parquet files to AWS S3",
			"In this example the batching mechanism of an `aws_s3` output collects raw messages consumed from a topic, and each batch is turned into one Parquet file.",
			`
output:
  aws_s3:
    bucket: TODO
    path: 'stuff/${! timestamp_unix() }-${! uuid_v4() }.parquet'
    batching:
      count: 100000
      period: 60s
      processors:
        - parquet_encode_protobuf:
            url: http://localhost:8081
            protobuf:
              use_proto_names: true
              serialize_to_json: false
            schema:
              - name: id
                type: INT64
                optional: true
              - name: content
                type: UTF8
                optional: true
            default_compression: zstd
`)
}

func init() {
	service.MustRegisterBatchProcessor(
		"parquet_encode_protobuf", parquetEncodeProtobufConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			decoder, err := newSchemaRegistryDecoderFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			encoder, err := parquet.NewEncodeProcessorFromConfig(conf, mgr.Logger())
			if err != nil {
				_ = decoder.Close(context.Background())
				return nil, err
			}
			return newParquetEncodeProtobufProcessor(decoder, encoder, mgr), nil
		})
}

//------------------------------------------------------------------------------

// directPlanKey identifies a message type within a schema registry schema.
type directPlanKey struct {
	id int
	// indexes is the message index path of the confluent framing, the empty
	// string being the first message of the schema.
	indexes string
}

// directPlanEntry is the cached outcome of planning a message type. A nil
// plan means the message type always goes through the regular path.
type directPlanEntry struct {
	plan *parquet.ProtobufPlan
}

type parquetEncodeProtobufProcessor struct {
	decoder *schemaRegistryDecoder
	encoder *parquet.EncodeProcessor
	logger  *service.Logger

	// disabled is the reason the direct path is never taken, if any.
	disabled string

	mDirect   *service.MetricCounter
	mFallback *service.MetricCounter

	plansMut sync.RWMutex
	plans    map[directPlanKey]*directPlanEntry
	// buildMut serialises schema lookups, so that batches whose plans are
	// already cached never wait on the registry.
	buildMut sync.Mutex

	loggedFallback atomic.Bool
}

func newParquetEncodeProtobufProcessor(decoder *schemaRegistryDecoder, encoder *parquet.EncodeProcessor, mgr *service.Resources) *parquetEncodeProtobufProcessor {
	p := &parquetEncodeProtobufProcessor{
		decoder:   decoder,
		encoder:   encoder,
		logger:    mgr.Logger(),
		mDirect:   mgr.Metrics().NewCounter(metricParquetProtobufDirect),
		mFallback: mgr.Metrics().NewCounter(metricParquetProtobufFallback),
		plans:     map[directPlanKey]*directPlanEntry{},
	}
	if decoder.cfg.protobuf.serializeToJSON {
		p.disabled = "protobuf.serialize_to_json is true"
		p.logger.Warnf("Batches will not be encoded directly from protobuf to parquet: %v", p.disabled)
	}
	return p
}

func (p *parquetEncodeProtobufProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	out, _, err := p.processBatch(ctx, batch)
	return out, err
}

// processBatch also reports whether the batch took the direct path.
func (p *parquetEncodeProtobufProcessor) processBatch(ctx context.Context, batch service.MessageBatch) (_ []service.MessageBatch, direct bool, _ error) {
	if len(batch) == 0 {
		return nil, false, nil
	}
	if p.disabled == "" {
		out, err := p.direct(ctx, batch)
		if err == nil {
			p.mDirect.Incr(1)
			return []service.MessageBatch{{out}}, true, nil
		}
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if p.loggedFallback.CompareAndSwap(false, true) {
			p.logger.Warnf("A batch could not be encoded directly from protobuf to parquet and was decoded and encoded the regular way instead, further occurrences are counted by the %v metric: %v", metricParquetProtobufFallback, err)
		} else {
			p.logger.Debugf("Batch decoded and encoded the regular way: %v", err)
		}
	}
	p.mFallback.Incr(1)
	out, err := p.regular(ctx, batch)
	return out, false, err
}

// direct encodes the batch straight from the protobuf wire format. It never
// modifies the batch, and any error means the regular path must be taken.
func (p *parquetEncodeProtobufProcessor) direct(ctx context.Context, batch service.MessageBatch) (*service.Message, error) {
	plans := make([]*parquet.ProtobufPlan, len(batch))
	data := make([][]byte, len(batch))

	var (
		ch       franz_sr.ConfluentHeader
		firstID  int
		lastKey  directPlanKey
		lastPlan *parquet.ProtobufPlan
	)
	for i, m := range batch {
		b, err := m.AsBytes()
		if err != nil {
			return nil, err
		}
		id, rest, err := ch.DecodeID(b)
		if err != nil {
			return nil, err
		}
		key := directPlanKey{id: id}
		if len(rest) > 0 && rest[0] == 0 {
			// The common framing of the first message of a schema.
			rest = rest[1:]
		} else {
			n, indexes, err := readMessageIndexes(rest)
			if err != nil {
				return nil, err
			}
			key.indexes = messageIndexesKey(indexes)
			rest = rest[n:]
		}
		if lastPlan == nil || key != lastKey {
			if lastPlan, err = p.plan(ctx, key); err != nil {
				return nil, err
			}
			lastKey = key
		}
		if i == 0 {
			firstID = id
		}
		plans[i] = lastPlan
		data[i] = rest
	}

	file, err := p.encoder.EncodeProtobuf(plans, data)
	if err != nil {
		return nil, err
	}

	// The regular path emits the first message of the decoded batch with its
	// contents replaced by the file.
	out := batch[0].Copy()
	out.SetBytes(file)
	out.MetaSetMut("schema_id", firstID)
	return out, nil
}

func messageIndexesKey(indexes []int) string {
	if len(indexes) == 1 && indexes[0] == 0 {
		return ""
	}
	var sb strings.Builder
	for i, idx := range indexes {
		if i > 0 {
			sb.WriteByte('.')
		}
		sb.WriteString(strconv.Itoa(idx))
	}
	return sb.String()
}

func (p *parquetEncodeProtobufProcessor) plan(ctx context.Context, key directPlanKey) (*parquet.ProtobufPlan, error) {
	entry, ok := p.cachedPlan(key)
	if !ok {
		var err error
		if entry, err = p.buildAndCachePlan(ctx, key); err != nil {
			return nil, err
		}
	}
	if entry.plan == nil {
		return nil, fmt.Errorf("schema ID %d does not qualify", key.id)
	}
	return entry.plan, nil
}

func (p *parquetEncodeProtobufProcessor) cachedPlan(key directPlanKey) (*directPlanEntry, bool) {
	p.plansMut.RLock()
	defer p.plansMut.RUnlock()
	entry, ok := p.plans[key]
	return entry, ok
}

func (p *parquetEncodeProtobufProcessor) buildAndCachePlan(ctx context.Context, key directPlanKey) (*directPlanEntry, error) {
	p.buildMut.Lock()
	defer p.buildMut.Unlock()

	// Another batch may have built the plan while this one waited.
	if entry, ok := p.cachedPlan(key); ok {
		return entry, nil
	}

	plan, reason, err := p.buildPlan(ctx, key)
	if err != nil {
		// Not cached: the registry may simply be unreachable, in which case
		// the regular path reports the failure.
		return nil, err
	}
	if plan == nil {
		p.logger.Warnf("Messages of schema ID %d are not encoded directly from protobuf to parquet and are decoded and encoded the regular way instead: %v", key.id, reason)
	}
	entry := &directPlanEntry{plan: plan}
	p.plansMut.Lock()
	p.plans[key] = entry
	p.plansMut.Unlock()
	return entry, nil
}

// buildPlan returns the plan for a message type, or the reason it has none.
// An error is returned for failures that may not repeat.
func (p *parquetEncodeProtobufProcessor) buildPlan(ctx context.Context, key directPlanKey) (plan *parquet.ProtobufPlan, reason string, err error) {
	ctx, done := context.WithTimeout(ctx, directSchemaFetchTimeout)
	defer done()

	schema, err := p.decoder.client.GetSchemaByID(ctx, key.id, false)
	if err != nil {
		return nil, "", err
	}
	if schema.Type != franz_sr.TypeProtobuf {
		return nil, "the schema is not a protobuf schema", nil
	}

	// Resolved the same way as the regular protobuf decoder.
	regMap := map[string]string{".": schema.Schema}
	if err := p.decoder.client.WalkReferences(ctx, schema.References, func(_ context.Context, name string, si franz_sr.Schema) error {
		regMap[name] = si.Schema
		return nil
	}); err != nil {
		return nil, "", err
	}
	files, _, err := common.RegistriesFromMap(regMap)
	if err != nil {
		return nil, fmt.Sprintf("parsing proto schema: %v", err), nil
	}
	targetFile, err := files.FindFileByPath(".")
	if err != nil {
		return nil, err.Error(), nil
	}

	indexes := []int{0}
	if key.indexes != "" {
		indexes = indexes[:0]
		for s := range strings.SplitSeq(key.indexes, ".") {
			idx, err := strconv.Atoi(s)
			if err != nil {
				return nil, "", err
			}
			indexes = append(indexes, idx)
		}
	}
	var md protoreflect.MessageDescriptor
	descriptors := targetFile.Messages()
	for _, idx := range indexes {
		if idx < 0 || idx >= descriptors.Len() {
			return nil, fmt.Sprintf("message index %d is out of range", idx), nil
		}
		md = descriptors.Get(idx)
		descriptors = md.Messages()
	}
	if md == nil {
		return nil, "the message has no message index", nil
	}

	opts := p.decoder.cfg.protobuf
	plan, err = p.encoder.NewProtobufPlan(md, parquet.ProtobufDirectOptions{
		UseProtoNames:     opts.useProtoNames,
		UseEnumNumbers:    opts.useEnumNumbers,
		EmitUnpopulated:   opts.emitUnpopulated,
		EmitDefaultValues: opts.emitDefaultValues,
	})
	if err != nil {
		return nil, err.Error(), nil
	}
	return plan, "", nil
}

// regular runs the batch through schema_registry_decode and then
// parquet_encode, reproducing what the two processors do when configured one
// after the other: a message that fails to decode is flagged and still
// reaches the encoder, and when encoding fails every message of the decoded
// batch is flagged.
func (p *parquetEncodeProtobufProcessor) regular(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	decoded := make(service.MessageBatch, 0, len(batch))
	for _, m := range batch {
		c := m.Copy()
		res, err := p.decoder.Process(ctx, c)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			c.SetError(err)
			decoded = append(decoded, c)
			continue
		}
		decoded = append(decoded, res...)
	}

	out, err := p.encoder.ProcessBatch(ctx, decoded)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		for i, m := range decoded {
			if i < len(batch) {
				// Reports the failure against this processor.
				batch[i].SetError(err)
			}
			m.SetError(err)
		}
		return []service.MessageBatch{decoded}, nil
	}
	return out, nil
}

func (p *parquetEncodeProtobufProcessor) Close(ctx context.Context) error {
	err := p.decoder.Close(ctx)
	if cerr := p.encoder.Close(ctx); err == nil {
		err = cerr
	}
	return err
}
