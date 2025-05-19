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

package avro

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/linkedin/goavro/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	sFieldRawJSON = "raw_json"
)

func avroScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Consume a stream of Avro OCF datum.").
		Description(`
== Avro JSON format

This scanner yields documents formatted as https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^] when decoding with Avro schemas. In this format the value of a union is encoded in JSON as follows:

- if its type is ` + "`null`, then it is encoded as a JSON `null`" + `;
- otherwise it is encoded as a JSON object with one name/value pair whose name is the type's name and whose value is the recursively encoded value. For Avro's named types (record, fixed or enum) the user-specified name is used, for other types the type name is used.

For example, the union schema ` + "`[\"null\",\"string\",\"Foo\"]`, where `Foo`" + ` is a record name, would encode:

- ` + "`null` as `null`" + `;
- the string ` + "`\"a\"` as `{\"string\": \"a\"}`" + `; and
- a ` + "`Foo` instance as `{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`" + ` instance.

However, it is possible to instead create documents in https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard/raw JSON format^] by setting the field ` + "<<avro_raw_json,`avro_raw_json`>> to `true`" + `.

This scanner also emits the canonical Avro schema as ` + "`@avro_schema`" + ` metadata, along with the schema's fingerprint available via ` + "`@avro_schema_fingerprint`" + `.
`).
		Fields(
			service.NewBoolField(sFieldRawJSON).
				Description("Whether messages should be decoded into normal JSON (\"json that meets the expectations of regular internet json\") rather than https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^]. If `true` the schema returned from the subject should be decoded as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard json^] instead of as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodec[avro json^]. There is a https://github.com/linkedin/goavro/blob/5ec5a5ee7ec82e16e6e2b438d610e1cab2588393/union.go#L224-L249[comment in goavro^], the https://github.com/linkedin/goavro[underlining library used for avro serialization^], that explains in more detail the difference between the standard json and avro json.").
				Advanced().
				Default(false),
		)
}

func init() {
	service.MustRegisterBatchScannerCreator("avro", avroScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return avroScannerFromParsed(conf)
		})
}

func avroScannerFromParsed(conf *service.ParsedConfig) (l *avroScannerCreator, err error) {
	l = &avroScannerCreator{}
	if l.rawJSON, err = conf.FieldBool(sFieldRawJSON); err != nil {
		return nil, err
	}
	return
}

type avroScannerCreator struct {
	rawJSON bool
}

func (c *avroScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	br := bufio.NewReader(rdr)
	ocf, err := goavro.NewOCFReader(br)
	if err != nil {
		return nil, err
	}

	ocfCodec := ocf.Codec()
	ocfSchema := ocfCodec.Schema()
	if c.rawJSON {
		if ocfCodec, err = goavro.NewCodecForStandardJSONFull(ocfSchema); err != nil {
			return nil, err
		}
	}

	return service.AutoAggregateBatchScannerAcks(&avroScanner{
		r:         rdr,
		ocf:       ocf,
		avroCodec: ocfCodec,
	}, aFn), nil
}

func (c *avroScannerCreator) Close(context.Context) error {
	return nil
}

type avroScanner struct {
	r         io.ReadCloser
	ocf       *goavro.OCFReader
	avroCodec *goavro.Codec
}

func (c *avroScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if c.r == nil {
		return nil, io.EOF
	}

	if !c.ocf.Scan() {
		err := c.ocf.Err()
		if err != nil {
			return nil, fmt.Errorf("failed to scan OCF file: %s", err)
		}
		return nil, io.EOF
	}

	datum, err := c.ocf.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read OCF datum: %s", err)
	}

	jb, err := c.avroCodec.TextualFromNative(nil, datum)
	if err != nil {
		return nil, fmt.Errorf("failed to decode OCF datum to JSON: %s", err)
	}
	msg := service.NewMessage(jb)
	msg.MetaSetMut("avro_schema", c.avroCodec.CanonicalSchema())
	msg.MetaSetMut("avro_schema_fingerprint", c.avroCodec.Rabin)
	return service.MessageBatch{msg}, nil
}

func (c *avroScanner) Close(ctx context.Context) error {
	if c.r == nil {
		return nil
	}
	return c.r.Close()
}
