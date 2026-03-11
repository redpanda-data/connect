// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package text

import (
	"bytes"
	"context"
	"strings"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	ssFieldDelimiter   = "delimiter"
	ssFieldEmitBytes   = "emit_bytes"
	ssFieldEmptyAsNull = "empty_as_null"
)

func init() {
	service.MustRegisterBatchProcessor("string_split", stringSplitSpec(), newStringSplit)
}

func stringSplitSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Parsing").
		Summary("Splits a string by a delimiter into an array.").
		Fields(
			service.NewStringField(ssFieldDelimiter).
				Default("\n").
				Description("The delimiter to split the string by."),
			service.NewBoolField(ssFieldEmitBytes).
				Default(false).
				Advanced().
				Description("When true, the output will be bloblang bytes instead of strings."),
			service.NewBoolField(ssFieldEmptyAsNull).
				Default(false).
				Description("When true, empty strings resulting from the split are converted to null."),
		)
}

type stringSplitProc struct {
	delimiter    []byte
	delimiterStr string

	emitBytes   bool
	emptyAsNull bool
}

func newStringSplit(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
	delimiter, err := conf.FieldString(ssFieldDelimiter)
	if err != nil {
		return nil, err
	}
	emitBytes, err := conf.FieldBool(ssFieldEmitBytes)
	if err != nil {
		return nil, err
	}
	emptyAsNull, err := conf.FieldBool(ssFieldEmptyAsNull)
	if err != nil {
		return nil, err
	}
	return &stringSplitProc{
		delimiter:    []byte(delimiter),
		delimiterStr: delimiter,
		emitBytes:    emitBytes,
		emptyAsNull:  emptyAsNull,
	}, nil
}

func (p *stringSplitProc) ProcessBatch(_ context.Context, input service.MessageBatch) ([]service.MessageBatch, error) {
	batch := make(service.MessageBatch, len(input))
	for i, msg := range input {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		var result []any
		if p.emitBytes {
			result = byteSplit(b, p.delimiter)
			if p.emptyAsNull {
				for i, v := range result {
					if s, ok := v.([]byte); ok && len(s) == 0 {
						result[i] = nil
					}
				}
			}
		} else {
			result = stringSplit(unsafe.String(unsafe.SliceData(b), len(b)), p.delimiterStr)
			if p.emptyAsNull {
				for i, v := range result {
					if s, ok := v.(string); ok && len(s) == 0 {
						result[i] = nil
					}
				}
			}
		}
		copy := msg.Copy()
		copy.SetStructuredMut(result)
		batch[i] = copy
	}
	return []service.MessageBatch{batch}, nil
}

func (*stringSplitProc) Close(context.Context) error { return nil }

func toAnySlice[T any](slice []T) []any {
	out := make([]any, len(slice))
	for i, v := range slice {
		out[i] = v
	}
	return out
}

func byteSplit(s []byte, sep []byte) []any {
	if len(sep) == 0 {
		return toAnySlice(bytes.Split(s, sep))
	}
	n := min(bytes.Count(s, sep)+1, len(s)+1)
	a := make([]any, n)
	n--
	i := 0
	for i < n {
		m := bytes.Index(s, sep)
		if m < 0 {
			break
		}
		a[i] = s[:m]
		s = s[m+len(sep):]
		i++
	}
	a[i] = s
	return a[:i+1]
}

func stringSplit(s string, sep string) []any {
	if len(sep) == 0 {
		return toAnySlice(strings.Split(s, sep))
	}
	n := min(strings.Count(s, sep)+1, len(s)+1)
	a := make([]any, n)
	n--
	i := 0
	for i < n {
		m := strings.Index(s, sep)
		if m < 0 {
			break
		}
		a[i] = s[:m]
		s = s[m+len(sep):]
		i++
	}
	a[i] = s
	return a[:i+1]
}
