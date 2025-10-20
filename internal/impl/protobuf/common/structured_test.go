/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package common

import (
	"encoding/json"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func loadTestDescriptors(t *testing.T) (protoreflect.FileDescriptor, protoreflect.MessageDescriptor, *protoregistry.Types) {
	t.Helper()
	mockResources := service.MockResources()
	files, types, err := loadDescriptors(mockResources.FS(), []string{"../../../../config/test/protobuf/schema"})
	require.NoError(t, err)

	fd, err := files.FindFileByPath("serde_test.proto")
	require.NoError(t, err)

	md := fd.Messages().ByName("SerdeTest")
	require.NotNil(t, md)

	return fd, md, types
}

// TestToMessageFastVsSlowEquivalent tests that ToMessageFast and ToMessageSlow produce
// the same JSON output for common cases where they should be equivalent.
func TestToMessageFastVsSlowEquivalent(t *testing.T) {
	_, md, types := loadTestDescriptors(t)

	tests := []struct {
		name      string
		textproto string
		opts      protojson.MarshalOptions
	}{
		{
			name: "basic string and int fields",
			textproto: `
				name: "test"
				count: 42
			`,
		},
		{
			name: "bool and double fields",
			textproto: `
				active: true
				price: 19.99
			`,
		},
		{
			name: "enum field",
			textproto: `
				status: STATUS_ACTIVE
			`,
		},
		{
			name: "enum with use_enum_numbers",
			textproto: `
				status: STATUS_ACTIVE
			`,
			opts: protojson.MarshalOptions{UseEnumNumbers: true},
		},
		{
			name: "repeated string field",
			textproto: `
				tags: "tag1"
				tags: "tag2"
				tags: "tag3"
			`,
		},
		{
			name: "repeated int field",
			textproto: `
				numbers: 1
				numbers: 2
				numbers: 3
			`,
		},
		{
			name: "map field",
			textproto: `
				metadata: {
					key: "key1"
					value: "value1"
				}
				metadata: {
					key: "key2"
					value: "value2"
				}
			`,
		},
		{
			name: "nested message",
			textproto: `
				nested: {
					inner_field: "nested_value"
					inner_count: 100
				}
			`,
		},
		{
			name: "all numeric types",
			textproto: `
				int32_val: 42
				uint32_val: 4294967295
				sint32_val: -42
				fixed32_val: 100
				sfixed32_val: -100
			`,
		},
		{
			name: "use proto names",
			textproto: `
				int32_val: 42
				uint32_val: 100
				nested: {
					inner_field: "test"
					inner_count: 99
				}
			`,
			opts: protojson.MarshalOptions{UseProtoNames: true},
		},
		{
			name: "normal float values",
			textproto: `
				price: 3.14159
			`,
		},
		{
			name: "google.protobuf.Any field",
			textproto: `
				any_field: {
					[type.googleapis.com/testing.SerdeTest.NestedMessage]: {
						inner_field: "packed in any"
						inner_count: 42
					}
				}
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a dynamic message and unmarshal from textproto
			pbMsg := dynamicpb.NewMessage(md)
			unmarshalOpts := prototext.UnmarshalOptions{
				Resolver: types,
			}
			err := unmarshalOpts.Unmarshal([]byte(tt.textproto), pbMsg)
			require.NoError(t, err)

			// Set up marshal options with resolver
			marshalOpts := tt.opts
			marshalOpts.Resolver = types

			// Convert using ToMessageFast
			fastMsg := service.NewMessage(nil)
			err = ToMessageFast(pbMsg, marshalOpts, fastMsg)
			require.NoError(t, err)

			// Convert using ToMessageSlow
			slowMsg := service.NewMessage(nil)
			err = ToMessageSlow(pbMsg, marshalOpts, slowMsg)
			require.NoError(t, err)

			// Get bytes from both messages
			fastBytes, err := fastMsg.AsBytes()
			require.NoError(t, err)

			slowBytes, err := slowMsg.AsBytes()
			require.NoError(t, err)

			// Compare JSON (ignoring formatting differences)
			assert.JSONEq(t, string(slowBytes), string(fastBytes),
				"ToMessageFast and ToMessageSlow should produce equivalent JSON for this case")
		})
	}
}

// TestToMessageFastVsSlowDifferences tests the documented edge cases where ToMessageFast
// and ToMessageSlow differ in their output.
func TestToMessageFastVsSlowDifferences(t *testing.T) {
	_, md, types := loadTestDescriptors(t)

	t.Run("google.protobuf.Timestamp preserved as time.Time", func(t *testing.T) {
		pbMsg := dynamicpb.NewMessage(md)
		unmarshalOpts := prototext.UnmarshalOptions{Resolver: types}
		err := unmarshalOpts.Unmarshal([]byte(`
			created_at: {
				seconds: 1234567890
				nanos: 123456789
			}
		`), pbMsg)
		require.NoError(t, err)

		// ToMessageFast preserves as time.Time
		fastMsg := service.NewMessage(nil)
		err = ToMessageFast(pbMsg, protojson.MarshalOptions{}, fastMsg)
		require.NoError(t, err)

		structured, err := fastMsg.AsStructured()
		require.NoError(t, err)

		structMap, ok := structured.(map[string]any)
		require.True(t, ok)

		createdAt, ok := structMap["createdAt"]
		require.True(t, ok, "createdAt field should be present")

		// ToMessageFast should preserve as time.Time
		_, isTime := createdAt.(time.Time)
		assert.True(t, isTime, "ToMessageFast should preserve timestamp as time.Time")

		// ToMessageSlow converts to string
		slowMsg := service.NewMessage(nil)
		err = ToMessageSlow(pbMsg, protojson.MarshalOptions{}, slowMsg)
		require.NoError(t, err)

		slowBytes, err := slowMsg.AsBytes()
		require.NoError(t, err)

		var slowStruct map[string]any
		err = json.Unmarshal(slowBytes, &slowStruct)
		require.NoError(t, err)

		slowCreatedAt, ok := slowStruct["createdAt"]
		require.True(t, ok)

		// ToMessageSlow should convert to RFC3339 string
		_, isString := slowCreatedAt.(string)
		assert.True(t, isString, "ToMessageSlow should convert timestamp to string")
	})

	t.Run("bytes preserved instead of base64 string", func(t *testing.T) {
		pbMsg := dynamicpb.NewMessage(md)
		unmarshalOpts := prototext.UnmarshalOptions{Resolver: types}
		err := unmarshalOpts.Unmarshal([]byte(`
			data: "\x01\x02\x03\xff\xfe"
		`), pbMsg)
		require.NoError(t, err)

		// ToMessageFast preserves as []byte
		fastMsg := service.NewMessage(nil)
		err = ToMessageFast(pbMsg, protojson.MarshalOptions{}, fastMsg)
		require.NoError(t, err)

		structured, err := fastMsg.AsStructured()
		require.NoError(t, err)

		structMap, ok := structured.(map[string]any)
		require.True(t, ok)

		data, ok := structMap["data"]
		require.True(t, ok)

		// ToMessageFast should preserve as []byte
		dataBytes, isBytes := data.([]byte)
		assert.True(t, isBytes, "ToMessageFast should preserve bytes as []byte")
		if isBytes {
			assert.Equal(t, []byte{0x01, 0x02, 0x03, 0xff, 0xfe}, dataBytes)
		}

		// ToMessageSlow converts to base64 string
		slowMsg := service.NewMessage(nil)
		err = ToMessageSlow(pbMsg, protojson.MarshalOptions{}, slowMsg)
		require.NoError(t, err)

		slowBytes, err := slowMsg.AsBytes()
		require.NoError(t, err)

		var slowStruct map[string]any
		err = json.Unmarshal(slowBytes, &slowStruct)
		require.NoError(t, err)

		slowData, ok := slowStruct["data"]
		require.True(t, ok)

		// ToMessageSlow should convert to base64 string
		_, isString := slowData.(string)
		assert.True(t, isString, "ToMessageSlow should convert bytes to base64 string")
	})

	t.Run("NaN, Infinity, -Infinity preserved as float", func(t *testing.T) {
		pbMsg := dynamicpb.NewMessage(md)
		unmarshalOpts := prototext.UnmarshalOptions{Resolver: types}
		err := unmarshalOpts.Unmarshal([]byte(`
			nan_value: nan
			inf_value: inf
			neg_inf_value: -inf
			float_nan: nan
			float_inf: inf
		`), pbMsg)
		require.NoError(t, err)

		// ToMessageFast preserves as float64
		fastMsg := service.NewMessage(nil)
		err = ToMessageFast(pbMsg, protojson.MarshalOptions{}, fastMsg)
		require.NoError(t, err)

		structured, err := fastMsg.AsStructured()
		require.NoError(t, err)

		structMap, ok := structured.(map[string]any)
		require.True(t, ok)

		// Check NaN
		nanVal, ok := structMap["nanValue"]
		require.True(t, ok)
		nanFloat, isFloat := nanVal.(float64)
		assert.True(t, isFloat, "ToMessageFast should preserve NaN as float64")
		if isFloat {
			assert.True(t, math.IsNaN(nanFloat), "NaN should be preserved as NaN")
		}

		// Check Infinity
		infVal, ok := structMap["infValue"]
		require.True(t, ok)
		infFloat, isFloat := infVal.(float64)
		assert.True(t, isFloat, "ToMessageFast should preserve Infinity as float64")
		if isFloat {
			assert.True(t, math.IsInf(infFloat, 1), "Infinity should be preserved as Infinity")
		}

		// Check -Infinity
		negInfVal, ok := structMap["negInfValue"]
		require.True(t, ok)
		negInfFloat, isFloat := negInfVal.(float64)
		assert.True(t, isFloat, "ToMessageFast should preserve -Infinity as float64")
		if isFloat {
			assert.True(t, math.IsInf(negInfFloat, -1), "-Infinity should be preserved as -Infinity")
		}

		// ToMessageSlow converts to strings
		slowMsg := service.NewMessage(nil)
		err = ToMessageSlow(pbMsg, protojson.MarshalOptions{}, slowMsg)
		require.NoError(t, err)

		slowBytes, err := slowMsg.AsBytes()
		require.NoError(t, err)

		var slowStruct map[string]any
		err = json.Unmarshal(slowBytes, &slowStruct)
		require.NoError(t, err)

		// In JSON, NaN and Infinity are represented as strings "NaN", "Infinity", "-Infinity"
		// when using standard JSON encoding
		slowNan, ok := slowStruct["nanValue"]
		require.True(t, ok)
		_, isString := slowNan.(string)
		assert.True(t, isString, "ToMessageSlow should convert NaN to string in JSON")
	})

	t.Run("unknown enum values emitted as default string", func(t *testing.T) {
		pbMsg := dynamicpb.NewMessage(md)
		unmarshalOpts := prototext.UnmarshalOptions{Resolver: types}
		err := unmarshalOpts.Unmarshal([]byte(`
			status: 100
		`), pbMsg)
		require.NoError(t, err)

		// ToMessageFast emits default enum name
		fastMsg := service.NewMessage(nil)
		err = ToMessageFast(pbMsg, protojson.MarshalOptions{}, fastMsg)
		require.NoError(t, err)

		structured, err := fastMsg.AsStructured()
		require.NoError(t, err)

		structMap, ok := structured.(map[string]any)
		require.True(t, ok)

		status, ok := structMap["status"]
		require.True(t, ok)

		// ToMessageFast should emit the default enum value name
		statusStr, isString := status.(string)
		assert.True(t, isString, "ToMessageFast should emit unknown enum as string")
		if isString {
			assert.Equal(t, "STATUS_UNSPECIFIED", statusStr, "Unknown enum should use default enum value name")
		}

		// ToMessageSlow emits the number
		slowMsg := service.NewMessage(nil)
		err = ToMessageSlow(pbMsg, protojson.MarshalOptions{}, slowMsg)
		require.NoError(t, err)

		slowBytes, err := slowMsg.AsBytes()
		require.NoError(t, err)

		var slowStruct map[string]any
		err = json.Unmarshal(slowBytes, &slowStruct)
		require.NoError(t, err)

		slowStatus, ok := slowStruct["status"]
		require.True(t, ok)

		// ToMessageSlow should emit the number for unknown enum
		statusNum, isNum := slowStatus.(float64) // JSON numbers are float64
		assert.True(t, isNum, "ToMessageSlow should emit unknown enum as number")
		if isNum {
			assert.Equal(t, float64(100), statusNum, "Unknown enum should be emitted as its numeric value")
		}
	})
}

// loadDescriptors is a helper function to load proto descriptors from import paths
// This matches the implementation in the parent package
func loadDescriptors(f fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files := map[string]string{}
	for _, importPath := range importPaths {
		if err := fs.WalkDir(f, importPath, func(path string, info fs.DirEntry, ferr error) error {
			if ferr != nil || info.IsDir() {
				return ferr
			}
			if filepath.Ext(info.Name()) == ".proto" && info.Name()[0] != '.' {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return ferr
				}
				content, ferr := os.ReadFile(path)
				if ferr != nil {
					return ferr
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return RegistriesFromMap(files)
}
