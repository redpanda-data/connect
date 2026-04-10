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

package arc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldURL            = "url"
	fieldToken          = "token"
	fieldDatabase       = "database"
	fieldMeasurement    = "measurement"
	fieldFormat         = "format"
	fieldTimestampField = "timestamp_field"
	fieldTimestampUnit  = "timestamp_unit"
	fieldTagsMapping    = "tags_mapping"
	fieldCompression    = "compression"
	fieldTLS            = "tls"
	fieldBatching       = "batching"
)

// Pools for compression writers to avoid expensive re-allocation per request.
var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			w, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
			return w
		},
	}
	gzipWriterPool = sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(nil, gzip.DefaultCompression)
			return w
		},
	}
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.88.0").
		Categories("Services").
		Summary("Writes data to an Arc database via the msgpack ingestion endpoint.").
		Description(`
This output sends data to an https://github.com/Basekick-Labs/arc[Arc^] columnar analytical database using its high-performance MessagePack ingestion endpoint.

Arc supports two payload formats:

- *columnar* (default): Transposes batched messages into column arrays. This is the recommended format, offering significantly faster ingestion.
- *row*: Sends each message as an individual row record with fields and optional tags.

Data is encoded as MessagePack and optionally compressed with zstd (recommended) or gzip before being sent to the Arc endpoint.
` + service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewURLField(fieldURL).
				Description("The base URL of the Arc instance.").
				Example("http://localhost:8000").
				Example("https://arc.example.com"),
			service.NewStringField(fieldToken).
				Secret().
				Optional().
				Description("Bearer token for authentication."),
			service.NewStringField(fieldDatabase).
				Default("default").
				Description("The target database name."),
			service.NewInterpolatedStringField(fieldMeasurement).
				Description("The measurement (table) name. Supports interpolation functions.").
				Example("cpu_metrics").
				Example(`${!metadata("measurement")}`).
				Example(`${!json("type")}`),
			service.NewStringEnumField(fieldFormat, "columnar", "row").
				Default("columnar").
				Description("The payload format. `columnar` transposes batch messages into column arrays for best performance. `row` sends each message as an individual record."),
			service.NewStringField(fieldTimestampField).
				Default("").
				Optional().
				Advanced().
				Description("The field name within each message containing the timestamp. If empty, the current time is used. Supports Unix timestamps and RFC3339 strings."),
			service.NewStringEnumField(fieldTimestampUnit, "us", "ms", "s", "ns", "auto").
				Default("auto").
				Advanced().
				Description("The unit of a numeric timestamp field. `auto` detects the unit based on magnitude. Ignored when `timestamp_field` is empty."),
			service.NewBloblangField(fieldTagsMapping).
				Optional().
				Description("An optional Bloblang mapping to extract tags from each message. Only used in `row` format. The result must be a `map[string]string`.").
				Example(`root = {"host": this.hostname, "region": this.region}`),
			service.NewStringEnumField(fieldCompression, "zstd", "gzip", "none").
				Default("zstd").
				Description("Compression algorithm for the request body. `zstd` is recommended for best decompression performance in Arc."),
			service.NewTLSToggledField(fieldTLS),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(fieldBatching),
		)
}

func init() {
	service.MustRegisterBatchOutput("arc", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newArcOutput(conf, mgr)
			return
		})
}

type arcOutput struct {
	log *service.Logger

	// Config
	url            string
	database       string
	measurement    *service.InterpolatedString
	format         string
	timestampField string
	timestampUnit  string
	tagsMapping    *bloblang.Executor
	compression    string

	// Pre-computed headers
	authHeader string

	// Runtime
	client   *http.Client
	endpoint string
}

func newArcOutput(conf *service.ParsedConfig, mgr *service.Resources) (*arcOutput, error) {
	o := &arcOutput{
		log: mgr.Logger(),
	}

	var err error
	if o.url, err = conf.FieldString(fieldURL); err != nil {
		return nil, err
	}
	o.url = strings.TrimRight(o.url, "/")
	o.endpoint = o.url + "/api/v1/write/msgpack"

	if conf.Contains(fieldToken) {
		token, err := conf.FieldString(fieldToken)
		if err != nil {
			return nil, err
		}
		if strings.ContainsAny(token, "\r\n") {
			return nil, errors.New("token contains invalid characters")
		}
		if token != "" {
			o.authHeader = "Bearer " + token
		}
	}

	if o.database, err = conf.FieldString(fieldDatabase); err != nil {
		return nil, err
	}
	if strings.ContainsAny(o.database, "\r\n") {
		return nil, errors.New("database name contains invalid characters")
	}

	if o.measurement, err = conf.FieldInterpolatedString(fieldMeasurement); err != nil {
		return nil, err
	}

	if o.format, err = conf.FieldString(fieldFormat); err != nil {
		return nil, err
	}

	if o.timestampField, err = conf.FieldString(fieldTimestampField); err != nil {
		return nil, err
	}

	if o.timestampUnit, err = conf.FieldString(fieldTimestampUnit); err != nil {
		return nil, err
	}

	if conf.Contains(fieldTagsMapping) {
		if o.tagsMapping, err = conf.FieldBloblang(fieldTagsMapping); err != nil {
			return nil, err
		}
	}

	if o.compression, err = conf.FieldString(fieldCompression); err != nil {
		return nil, err
	}

	// Set up HTTP client with optional TLS
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:  10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	tlsConf, tlsEnabled, err := conf.FieldTLSToggled(fieldTLS)
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		transport.TLSClientConfig = tlsConf
	}
	o.client = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			return nil
		},
	}

	return o, nil
}

func (o *arcOutput) Connect(_ context.Context) error {
	return nil
}

func (o *arcOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	var payload any
	var err error

	switch o.format {
	case "columnar":
		payload, err = o.buildColumnarPayload(batch)
	case "row":
		payload, err = o.buildRowPayload(batch)
	default:
		return fmt.Errorf("unsupported format: %s", o.format)
	}
	if err != nil {
		return err
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		return fmt.Errorf("msgpack encoding: %w", err)
	}

	compressed, encoding, err := o.compress(data)
	if err != nil {
		return fmt.Errorf("compression: %w", err)
	}

	return o.sendRequest(ctx, compressed, encoding)
}

func (o *arcOutput) buildColumnarPayload(batch service.MessageBatch) (any, error) {
	measExec := batch.InterpolationExecutor(o.measurement)

	type columnarRecord struct {
		columns map[string][]any
	}

	// Group messages by measurement, preserving insertion order
	groups := make(map[string]*columnarRecord)
	var order []string

	for i, msg := range batch {
		measName, err := measExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("measurement interpolation: %w", err)
		}

		data, err := msg.AsStructuredMut()
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message %d: expected object, got %T", i, data)
		}

		ts := o.extractTimestamp(dataMap)

		rec, exists := groups[measName]
		if !exists {
			rec = &columnarRecord{columns: make(map[string][]any)}
			groups[measName] = rec
			order = append(order, measName)
		}

		rec.columns["time"] = append(rec.columns["time"], ts)
		for k, v := range dataMap {
			if k == o.timestampField || (o.timestampField == "" && k == "time") {
				continue
			}
			rec.columns[k] = append(rec.columns[k], v)
		}
	}

	// Build payload
	records := make([]any, 0, len(order))
	for _, name := range order {
		rec := groups[name]
		records = append(records, map[string]any{
			"m":       name,
			"columns": rec.columns,
		})
	}

	if len(records) == 1 {
		return records[0], nil
	}
	return map[string]any{"batch": records}, nil
}

func (o *arcOutput) buildRowPayload(batch service.MessageBatch) (any, error) {
	measExec := batch.InterpolationExecutor(o.measurement)

	var tagsExec *service.MessageBatchBloblangExecutor
	if o.tagsMapping != nil {
		tagsExec = batch.BloblangExecutor(o.tagsMapping)
	}

	rows := make([]any, 0, len(batch))
	for i, msg := range batch {
		measName, err := measExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("measurement interpolation: %w", err)
		}

		data, err := msg.AsStructuredMut()
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message %d: expected object, got %T", i, data)
		}

		ts := o.extractTimestamp(dataMap)

		// Remove timestamp field from fields if present
		fields := make(map[string]any, len(dataMap))
		for k, v := range dataMap {
			if k == o.timestampField || (o.timestampField == "" && k == "time") {
				continue
			}
			fields[k] = v
		}

		row := map[string]any{
			"m":      measName,
			"t":      ts,
			"fields": fields,
		}

		// Extract tags via bloblang mapping
		if tagsExec != nil {
			tagMsg, err := tagsExec.Query(i)
			if err != nil {
				return nil, fmt.Errorf("tags mapping: %w", err)
			}
			if tagMsg != nil {
				tagResult, err := tagMsg.AsStructured()
				if err != nil {
					return nil, fmt.Errorf("tags mapping result: %w", err)
				}
				if tagMap, ok := tagResult.(map[string]any); ok {
					tags := make(map[string]string, len(tagMap))
					for k, v := range tagMap {
						tags[k] = fmt.Sprintf("%v", v)
					}
					row["tags"] = tags
				}
			}
		}

		rows = append(rows, row)
	}

	if len(rows) == 1 {
		return rows[0], nil
	}
	return rows, nil
}

func (o *arcOutput) extractTimestamp(data map[string]any) int64 {
	if o.timestampField == "" {
		return time.Now().UnixMicro()
	}

	val, ok := data[o.timestampField]
	if !ok {
		return time.Now().UnixMicro()
	}

	switch v := val.(type) {
	case int64:
		return convertTimestamp(v, o.timestampUnit)
	case int:
		return convertTimestamp(int64(v), o.timestampUnit)
	case float64:
		return convertTimestamp(int64(v), o.timestampUnit)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return convertTimestamp(i, o.timestampUnit)
		}
		if f, err := v.Float64(); err == nil {
			return convertTimestamp(int64(f), o.timestampUnit)
		}
	case string:
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UnixMicro()
		}
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UnixMicro()
		}
	}

	return time.Now().UnixMicro()
}

func convertTimestamp(v int64, unit string) int64 {
	switch unit {
	case "us":
		return v
	case "ms":
		return v * 1000
	case "s":
		return v * 1_000_000
	case "ns":
		return v / 1000
	case "auto":
		abs := v
		if abs < 0 {
			abs = -abs
		}
		switch {
		case abs >= int64(1e16):
			return v / 1000 // nanoseconds
		case abs >= int64(1e13):
			return v // microseconds
		case abs >= int64(1e10):
			return v * 1000 // milliseconds
		default:
			return v * 1_000_000 // seconds
		}
	}
	return v
}

func (o *arcOutput) compress(data []byte) ([]byte, string, error) {
	switch o.compression {
	case "none":
		return data, "", nil
	case "zstd":
		var buf bytes.Buffer
		w := zstdEncoderPool.Get().(*zstd.Encoder)
		w.Reset(&buf)
		if _, err := w.Write(data); err != nil {
			w.Close()
			return nil, "", err
		}
		if err := w.Close(); err != nil {
			return nil, "", err
		}
		zstdEncoderPool.Put(w)
		return buf.Bytes(), "zstd", nil
	case "gzip":
		var buf bytes.Buffer
		w := gzipWriterPool.Get().(*gzip.Writer)
		w.Reset(&buf)
		if _, err := w.Write(data); err != nil {
			w.Close()
			return nil, "", err
		}
		if err := w.Close(); err != nil {
			return nil, "", err
		}
		gzipWriterPool.Put(w)
		return buf.Bytes(), "gzip", nil
	}
	return data, "", nil
}

func (o *arcOutput) sendRequest(ctx context.Context, body []byte, contentEncoding string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/msgpack")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	req.Header.Set("x-arc-database", o.database)
	if o.authHeader != "" {
		req.Header.Set("Authorization", o.authHeader)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var errResp struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("arc responded with %d: %s", resp.StatusCode, errResp.Error)
	}
	return fmt.Errorf("arc responded with status %d: %s", resp.StatusCode, string(respBody))
}

func (o *arcOutput) Close(_ context.Context) error {
	o.client.CloseIdleConnections()
	return nil
}
