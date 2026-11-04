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

package turbopuffer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"strconv"
	"strings"

	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
	"github.com/turbopuffer/turbopuffer-go/v2/option"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldAPIKey          = "api_key"
	fieldRegion          = "region"
	fieldBaseURL         = "base_url"
	fieldMaxRetries      = "max_retries"
	fieldNamespace       = "namespace"
	fieldOperation       = "operation"
	fieldID              = "id"
	fieldDocumentMapping = "document_mapping"
	fieldSchema          = "schema"
	fieldDistanceMetric  = "distance_metric"
	fieldBatching        = "batching"
)

const (
	operationUpsert = "upsert"
	operationPatch  = "patch"
	operationDelete = "delete"
)

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.117.0").
		Categories("AI", "Services").
		Summary("Writes documents to a turbopuffer namespace.").
		Description(`This output batches document upserts, patches, and deletes into turbopuffer namespace write requests.

The `+"`namespace`"+` and `+"`operation`"+` fields are interpolated for each message. A batch can therefore target multiple namespaces and contain a mixture of operations. Requests with duplicate document IDs are split automatically because turbopuffer requires IDs to be unique within a write request.

Writes are delivered at least once. Use stable document IDs and set `+"`max_in_flight: 1`"+` when writes to the same document must retain input order.`+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewStringField(fieldAPIKey).
				Secret().
				Description("The turbopuffer API key."),
			service.NewStringField(fieldRegion).
				Description("The turbopuffer region to write to.").
				Example("gcp-us-central1").
				Example("aws-us-east-1"),
			service.NewStringField(fieldBaseURL).
				Default("").
				Advanced().
				LintRule(`root = if this != "" && !this.contains("REGION") { ["must contain a REGION placeholder"] }`).
				Description("An optional custom turbopuffer API base URL. This is primarily useful for tests and proxies and must contain a `REGION` placeholder."),
			service.NewIntField(fieldMaxRetries).
				Default(2).
				Advanced().
				LintRule(`root = if this < 0 { ["must not be negative"] }`).
				Description("The maximum number of retries performed by the turbopuffer client for transient failures."),
			service.NewInterpolatedStringField(fieldNamespace).
				Description("The turbopuffer namespace to write to.").
				Example("wikipedia").
				Example(`${! meta("namespace") }`),
			service.NewInterpolatedStringField(fieldOperation).
				Default(operationUpsert).
				Description("The operation to perform. Must resolve to `upsert`, `patch`, or `delete`.").
				Example(`${! meta("operation") }`),
			service.NewBloblangField(fieldID).
				Description("A Bloblang mapping that extracts a string or unsigned integer document ID.").
				Example("root = this.id"),
			service.NewBloblangField(fieldDocumentMapping).
				Default("root = this").
				Description("A Bloblang mapping that extracts the document attributes for upsert and patch operations. The configured ID overwrites any `id` attribute returned by this mapping.").
				Example("root = this.document"),
			service.NewAnyField(fieldSchema).
				Optional().
				Advanced().
				Description("An optional turbopuffer namespace schema sent with each write request."),
			service.NewStringEnumField(fieldDistanceMetric, string(tpuf.DistanceMetricCosineDistance), string(tpuf.DistanceMetricEuclideanSquared)).
				Optional().
				Advanced().
				Description("An optional distance metric for vector attributes in the namespace."),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(fieldBatching),
		).
		Example("Wikipedia documents", "Upserts Wikipedia documents in batches. Set max_in_flight to one when messages for the same page must remain ordered.", `
output:
  turbopuffer:
    api_key: ${TURBOPUFFER_API_KEY}
    region: gcp-us-central1
    namespace: wikipedia
    id: root = this.page_id
    document_mapping: |
      root.title = this.title
      root.text = this.text
      root.url = this.url
      root.revision_id = this.revision_id
      root.updated_at = this.timestamp
    schema:
      title:
        type: string
        full_text_search: true
      text:
        type: string
        full_text_search: true
      url:
        type: string
      revision_id:
        type: uint
      updated_at:
        type: datetime
    max_in_flight: 1
    batching:
      count: 100
      period: 1s
`)
}

func init() {
	service.MustRegisterBatchOutput("turbopuffer", outputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
		if batchPolicy, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
			return
		}
		if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
			return
		}
		out, err = newOutputWriter(conf, mgr)
		return
	})
}

type outputWriter struct {
	client client

	namespace       *service.InterpolatedString
	operation       *service.InterpolatedString
	id              *bloblang.Executor
	documentMapping *bloblang.Executor
	schema          map[string]tpuf.AttributeSchemaParam
	distanceMetric  tpuf.DistanceMetric
}

func newOutputWriter(conf *service.ParsedConfig, _ *service.Resources) (*outputWriter, error) {
	apiKey, err := conf.FieldString(fieldAPIKey)
	if err != nil {
		return nil, err
	}
	region, err := conf.FieldString(fieldRegion)
	if err != nil {
		return nil, err
	}
	baseURL, err := conf.FieldString(fieldBaseURL)
	if err != nil {
		return nil, err
	}
	if baseURL != "" && !strings.Contains(baseURL, "REGION") {
		return nil, fmt.Errorf("%s must contain a REGION placeholder", fieldBaseURL)
	}
	maxRetries, err := conf.FieldInt(fieldMaxRetries)
	if err != nil {
		return nil, err
	}

	clientOptions := []option.RequestOption{
		option.WithAPIKey(apiKey),
		option.WithRegion(region),
		option.WithMaxRetries(maxRetries),
	}
	if baseURL != "" {
		clientOptions = append(clientOptions, option.WithBaseURL(baseURL))
	}
	sdk := tpuf.NewClient(clientOptions...)

	namespace, err := conf.FieldInterpolatedString(fieldNamespace)
	if err != nil {
		return nil, err
	}
	operation, err := conf.FieldInterpolatedString(fieldOperation)
	if err != nil {
		return nil, err
	}
	id, err := conf.FieldBloblang(fieldID)
	if err != nil {
		return nil, err
	}
	documentMapping, err := conf.FieldBloblang(fieldDocumentMapping)
	if err != nil {
		return nil, err
	}

	var schema map[string]tpuf.AttributeSchemaParam
	if conf.Contains(fieldSchema) {
		rawSchema, err := conf.FieldAny(fieldSchema)
		if err != nil {
			return nil, err
		}
		if schema, err = parseSchema(rawSchema); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", fieldSchema, err)
		}
	}

	var distanceMetric tpuf.DistanceMetric
	if conf.Contains(fieldDistanceMetric) {
		rawDistanceMetric, err := conf.FieldString(fieldDistanceMetric)
		if err != nil {
			return nil, err
		}
		distanceMetric = tpuf.DistanceMetric(rawDistanceMetric)
	}

	return &outputWriter{
		client:          &sdkClient{client: sdk},
		namespace:       namespace,
		operation:       operation,
		id:              id,
		documentMapping: documentMapping,
		schema:          schema,
		distanceMetric:  distanceMetric,
	}, nil
}

func parseSchema(raw any) (map[string]tpuf.AttributeSchemaParam, error) {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var schema map[string]tpuf.AttributeSchemaParam
	if err := json.Unmarshal(encoded, &schema); err != nil {
		return nil, err
	}
	return schema, nil
}

func (w *outputWriter) Connect(ctx context.Context) error {
	if err := w.client.CheckConnection(ctx); err != nil {
		return fmt.Errorf("connecting to turbopuffer: %w", err)
	}
	return nil
}

type pendingWrite struct {
	namespace string
	params    tpuf.NamespaceWriteParams
	indices   []int
	ids       map[string]struct{}
}

func (w *outputWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	requests, err := w.buildRequests(batch)
	if err != nil {
		return err
	}

	var batchErr *service.BatchError
	failedNamespaces := map[string]error{}
	for _, request := range requests {
		writeErr := failedNamespaces[request.namespace]
		if writeErr == nil {
			writeErr = w.client.Write(ctx, request.namespace, request.params)
			if writeErr != nil {
				failedNamespaces[request.namespace] = writeErr
			}
		}
		if writeErr != nil {
			if batchErr == nil {
				batchErr = service.NewBatchError(batch, writeErr)
			}
			for _, index := range request.indices {
				batchErr.Failed(index, writeErr)
			}
		}
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (w *outputWriter) buildRequests(batch service.MessageBatch) ([]pendingWrite, error) {
	namespaceExec := batch.InterpolationExecutor(w.namespace)
	operationExec := batch.InterpolationExecutor(w.operation)

	var requests []pendingWrite
	currentRequest := map[string]int{}
	for i := range batch {
		input, err := batch[i].AsStructured()
		if err != nil {
			return nil, fmt.Errorf("parsing message %d as structured data: %w", i, err)
		}
		namespace, err := namespaceExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", fieldNamespace, err)
		}
		if !namespacePattern.MatchString(namespace) {
			return nil, fmt.Errorf("%s %q must match [A-Za-z0-9._-]{1,128}", fieldNamespace, namespace)
		}

		operation, err := operationExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", fieldOperation, err)
		}
		if operation != operationUpsert && operation != operationPatch && operation != operationDelete {
			return nil, fmt.Errorf("%s %q must be one of upsert, patch, or delete", fieldOperation, operation)
		}

		rawID, err := w.id.Query(input)
		if err != nil {
			return nil, fmt.Errorf("executing %s: %w", fieldID, err)
		}
		id, idKey, err := normalizeID(rawID)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", fieldID, err)
		}

		requestIndex, exists := currentRequest[namespace]
		if exists {
			if _, duplicate := requests[requestIndex].ids[idKey]; duplicate {
				exists = false
			}
		}
		if !exists {
			requestIndex = len(requests)
			currentRequest[namespace] = requestIndex
			requests = append(requests, pendingWrite{
				namespace: namespace,
				params: tpuf.NamespaceWriteParams{
					Schema:         w.schema,
					DistanceMetric: w.distanceMetric,
				},
				ids: map[string]struct{}{},
			})
		}

		request := &requests[requestIndex]
		request.indices = append(request.indices, i)
		request.ids[idKey] = struct{}{}
		switch operation {
		case operationDelete:
			request.params.Deletes = append(request.params.Deletes, id)
		case operationUpsert, operationPatch:
			rawDocument, err := w.documentMapping.Query(input)
			if err != nil {
				return nil, fmt.Errorf("executing %s: %w", fieldDocumentMapping, err)
			}
			document, ok := rawDocument.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s must return an object, got %T", fieldDocumentMapping, rawDocument)
			}
			row := make(tpuf.RowParam, len(document)+1)
			maps.Copy(row, document)
			row["id"] = id
			if operation == operationUpsert {
				request.params.UpsertRows = append(request.params.UpsertRows, row)
			} else {
				request.params.PatchRows = append(request.params.PatchRows, row)
			}
		}
	}
	return requests, nil
}

func normalizeID(raw any) (id any, key string, err error) {
	switch value := raw.(type) {
	case string:
		if len(value) > 64 {
			return nil, "", errors.New("string document IDs must not exceed 64 bytes")
		}
		return value, "s:" + value, nil
	case int:
		if value < 0 {
			return nil, "", errors.New("integer document IDs must not be negative")
		}
		return normalizeUint(uint64(value))
	case int8:
		if value < 0 {
			return nil, "", errors.New("integer document IDs must not be negative")
		}
		return normalizeUint(uint64(value))
	case int16:
		if value < 0 {
			return nil, "", errors.New("integer document IDs must not be negative")
		}
		return normalizeUint(uint64(value))
	case int32:
		if value < 0 {
			return nil, "", errors.New("integer document IDs must not be negative")
		}
		return normalizeUint(uint64(value))
	case int64:
		if value < 0 {
			return nil, "", errors.New("integer document IDs must not be negative")
		}
		return normalizeUint(uint64(value))
	case uint:
		return normalizeUint(uint64(value))
	case uint8:
		return normalizeUint(uint64(value))
	case uint16:
		return normalizeUint(uint64(value))
	case uint32:
		return normalizeUint(uint64(value))
	case uint64:
		return normalizeUint(value)
	case json.Number:
		parsed, parseErr := strconv.ParseUint(value.String(), 10, 64)
		if parseErr != nil {
			return nil, "", errors.New("numeric document IDs must be unsigned 64-bit integers")
		}
		return normalizeUint(parsed)
	case float64:
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value >= math.Exp2(64) {
			return nil, "", errors.New("numeric document IDs must be unsigned 64-bit integers")
		}
		return normalizeUint(uint64(value))
	default:
		return nil, "", fmt.Errorf("document IDs must be strings or unsigned integers, got %T", raw)
	}
}

func normalizeUint(value uint64) (id any, key string, err error) {
	return value, fmt.Sprintf("u:%d", value), nil
}

func (*outputWriter) Close(context.Context) error {
	return nil
}
