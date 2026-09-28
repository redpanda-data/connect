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

package avro

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	sFieldRawJSON                   = "raw_json"
	sFieldMaxDecompressedBlockBytes = "max_decompressed_block_bytes"
)

// defaultMaxDecompressedBlockBytes bounds the decompressed size of a single
// Avro OCF block. It defends against decompression-amplification ("deflate
// bomb") inputs, where a tiny compressed block expands into a huge
// allocation. Legitimate OCF blocks are typically tens of KB to a few MB, so
// this leaves ample headroom while capping worst-case memory: a rejected
// block never reaches the subsequent Avro->JSON encoding step, which itself
// further expands an accepted block (observed ~6x on the researcher's PoC
// datum, largely from decimal/bytes fields spelling out as JSON). Worst-case
// resident size for one accepted block is therefore on the order of
// defaultMaxDecompressedBlockBytes * 6 (~100 MB at the default), not the
// 16 MiB cap alone — accepted as proportionate headroom over real OCF block
// sizes when this default was chosen.
const defaultMaxDecompressedBlockBytes = 16 << 20 // 16 MiB

// effectiveMaxDecompressedBlockBytes resolves the byte cap this scanner
// passes to ocf.WithMaxDecompressedBlockBytes for a given configured field
// value. It intercepts <= 0 itself (LintRule already rejects < 0, so in
// practice this only ever normalizes 0) rather than forwarding it, because
// the underlying twmb/avro reader has its own, DIFFERENT fallback for <= 0
// — its own built-in 64 MiB default (ocf.go: "The default is 64 MiB"),
// which is *larger* than this field's 16 MiB default. Forwarding 0 verbatim
// would make the field's documented "use the default" sentinel silently
// raise the effective cap to 4x its stated value instead of leaving it at
// this field's own default — do not remove this interception without
// re-checking that upstream behaviour.
func effectiveMaxDecompressedBlockBytes(configured int) int {
	if configured <= 0 {
		return defaultMaxDecompressedBlockBytes
	}
	return configured
}

func avroScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Consume a stream of Avro OCF datum.").
		Description(`
== Avro JSON format

This scanner yields documents formatted as https://avro.apache.org/docs/current/specification/#json-encoding[Avro JSON^] when decoding with Avro schemas. In this format the value of a union is encoded in JSON as follows:

- if its type is `+"`null`, then it is encoded as a JSON `null`"+`;
- otherwise it is encoded as a JSON object with one name/value pair whose name is the type's name and whose value is the recursively encoded value. For Avro's named types (record, fixed or enum) the user-specified name is used, for other types the type name is used.

For example, the union schema `+"`[\"null\",\"string\",\"Foo\"]`, where `Foo`"+` is a record name, would encode:

- `+"`null` as `null`"+`;
- the string `+"`\"a\"` as `{\"string\": \"a\"}`"+`; and
- a `+"`Foo` instance as `{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`"+` instance.

However, it is possible to instead create documents in standard/raw JSON format by setting the field `+"<<avro_raw_json,`avro_raw_json`>> to `true`"+`.

This scanner also emits the canonical Avro schema as `+"`@avro_schema`"+` metadata, along with the schema's fingerprint available via `+"`@avro_schema_fingerprint`"+`.
`).
		Fields(
			service.NewBoolField(sFieldRawJSON).
				Description("Whether messages should be decoded into normal JSON rather than https://avro.apache.org/docs/current/specification/#json-encoding[Avro JSON^]. When true, union values are unwrapped (bare values instead of {\"type\": value} wrappers).").
				Advanced().
				Default(false),
			service.NewIntField(sFieldMaxDecompressedBlockBytes).
				Description("The maximum size, in bytes, that a single compressed Avro OCF block is allowed to expand to when decompressed. This guards against decompression-amplification (\"deflate bomb\") inputs, where a tiny compressed block would otherwise expand into a very large allocation. A block that exceeds this limit causes the scan to fail rather than be materialized. There is deliberately no way to disable this cap: pipelines with legitimately larger blocks should set an explicit higher value. Set to `0` to use the default (equivalent to omitting the field).").
				Advanced().
				Default(defaultMaxDecompressedBlockBytes).
				LintRule(`root = if this < 0 { [ "`+sFieldMaxDecompressedBlockBytes+` must be >= 0" ] }`),
		)
}

func init() {
	service.MustRegisterBatchScannerCreator("avro", avroScannerSpec(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchScannerCreator, error) {
			return avroScannerFromParsed(conf)
		})
}

func avroScannerFromParsed(conf *service.ParsedConfig) (l *avroScannerCreator, err error) {
	l = &avroScannerCreator{}
	if l.rawJSON, err = conf.FieldBool(sFieldRawJSON); err != nil {
		return nil, err
	}
	if l.maxDecompressedBlockBytes, err = conf.FieldInt(sFieldMaxDecompressedBlockBytes); err != nil {
		return nil, err
	}
	return
}

type avroScannerCreator struct {
	rawJSON                   bool
	maxDecompressedBlockBytes int
}

func (c *avroScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, _ *service.ScannerSourceDetails) (service.BatchScanner, error) {
	reader, err := ocf.NewReader(rdr,
		ocf.WithMaxDecompressedBlockBytes(int64(effectiveMaxDecompressedBlockBytes(c.maxDecompressedBlockBytes))),
	)
	if err != nil {
		return nil, err
	}

	schema := reader.Schema()
	canonical := string(schema.Canonical())
	fp := binary.BigEndian.Uint64(schema.Fingerprint(avro.NewRabin()))

	return service.AutoAggregateBatchScannerAcks(&avroScanner{
		r:         rdr,
		reader:    reader,
		schema:    schema,
		rawJSON:   c.rawJSON,
		canonical: canonical,
		fp:        fp,
	}, aFn), nil
}

func (*avroScannerCreator) Close(context.Context) error {
	return nil
}

type avroScanner struct {
	r         io.ReadCloser
	reader    *ocf.Reader
	schema    *avro.Schema
	rawJSON   bool
	canonical string
	fp        uint64
}

func (c *avroScanner) NextBatch(context.Context) (service.MessageBatch, error) {
	if c.r == nil {
		return nil, io.EOF
	}

	var native any
	if err := c.reader.Decode(&native); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("reading OCF datum: %s", err)
	}

	var jb []byte
	var err error
	if c.rawJSON {
		jb, err = c.schema.EncodeJSON(native, avro.LinkedinFloats())
	} else {
		jb, err = c.schema.EncodeJSON(native, avro.TaggedUnions(), avro.TagLogicalTypes(), avro.LinkedinFloats())
	}
	if err != nil {
		return nil, fmt.Errorf("encoding OCF datum to JSON: %s", err)
	}

	msg := service.NewMessage(jb)
	msg.MetaSetMut("avro_schema", c.canonical)
	msg.MetaSetMut("avro_schema_fingerprint", c.fp)
	return service.MessageBatch{msg}, nil
}

func (c *avroScanner) Close(context.Context) error {
	if c.r == nil {
		return nil
	}
	readerErr := c.reader.Close() // closes codec only, not the underlying reader
	rErr := c.r.Close()
	return errors.Join(readerErr, rErr)
}
