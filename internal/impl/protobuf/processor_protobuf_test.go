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

// This file contains code originally licensed under the MIT License:

// Copyright (c) 2024-present Bento contributors

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package protobuf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	v1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestProtobufFromJSON(t *testing.T) {
	type testCase struct {
		name           string
		message        string
		importPath     string
		input          string
		outputContains []string
		discardUnknown bool
	}

	tests := []testCase{
		{
			name:           "json to protobuf age",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"john","lastName":"oates","age":10}`,
			outputContains: []string{"john"},
		},
		{
			name:           "json to protobuf min",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"daryl","lastName":"hall"}`,
			outputContains: []string{"daryl"},
		},
		{
			name:           "json to protobuf email",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com"}`,
			outputContains: []string{"caleb"},
		},
		{
			name:           "json to protobuf with discard_unknown",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"firstName":"caleb","lastName":"quaye","missingfield":"anyvalue"}`,
			outputContains: []string{"caleb"},
			discardUnknown: true,
		},
		{
			name:           "any: json to protobuf 1",
			message:        "testing.Envelope",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","first_name":"bob"}}`,
			outputContains: []string{"type.googleapis.com/testing.Person"},
		},
		{
			name:           "any: json to protobuf 2",
			message:        "testing.Envelope",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`,
			outputContains: []string{"type.googleapis.com/testing.House"},
		},
		{
			name:           "any: json to protobuf with nested message",
			message:        "testing.House.Mailbox",
			importPath:     "../../../config/test/protobuf/schema",
			input:          `{"color":"red","identifier":"123"}`,
			outputContains: []string{"red"},
		},
	}

	for i, test := range tests {
		t.Run(test.name+"/"+strconv.Itoa(i), func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: from_json
message: %v
import_paths: [ %v ]
discard_unknown: %t
`, test.message, test.importPath, test.discardUnknown), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(t.Context(), service.NewMessage([]byte(test.input)))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.NotEqual(t, test.input, string(mBytes))
			for _, exp := range test.outputContains {
				assert.Contains(t, string(mBytes), exp)
			}
			require.NoError(t, msgs[0].GetError())
		})

		t.Run(test.name+" bsr", func(t *testing.T) {
			mockBSRServerAddress := runMockBSRServer(t, test.importPath)

			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: from_json
message: %v
bsr:
  - module: "testing"
    url: %s
discard_unknown: %t
`, test.message, "http://"+mockBSRServerAddress, test.discardUnknown), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(t.Context(), service.NewMessage([]byte(test.input)))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.NotEqual(t, test.input, string(mBytes))
			for _, exp := range test.outputContains {
				assert.Contains(t, string(mBytes), exp)
			}
			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestProtobufToJSON(t *testing.T) {
	type testCase struct {
		name           string
		message        string
		importPath     string
		input          []byte
		output         string
		useProtoNames  bool
		useEnumNumbers bool
	}

	tests := []testCase{
		{
			name:       "protobuf to json 1",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      []byte{0x0a, 0x04, 0x6a, 0x6f, 0x68, 0x6e, 0x12, 0x05, 0x6f, 0x61, 0x74, 0x65, 0x73, 0x20, 0x0a},
			output:     `{"firstName":"john","lastName":"oates","age":10}`,
		},
		{
			name:       "protobuf to json 2",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      []byte{0x0a, 0x05, 0x64, 0x61, 0x72, 0x79, 0x6c, 0x12, 0x04, 0x68, 0x61, 0x6c, 0x6c},
			output:     `{"firstName":"daryl","lastName":"hall"}`,
		},
		{
			name:       "protobuf to json 3",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d, 0x40, 0x01,
			},
			output: `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com","device":"DEVICE_IOS"}`,
		},
		{
			name:          "protobuf to json with use_proto_names",
			message:       "testing.Person",
			importPath:    "../../../config/test/protobuf/schema",
			useProtoNames: true,
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d,
			},
			output: `{"first_name":"caleb","last_name":"quaye","email":"caleb@myspace.com"}`,
		},
		{
			name:           "protobuf to json with use_enum_numbers",
			message:        "testing.Person",
			importPath:     "../../../config/test/protobuf/schema",
			useEnumNumbers: true,
			input: []byte{
				0x0a, 0x05, 0x63, 0x61, 0x6c, 0x65, 0x62, 0x12, 0x05, 0x71, 0x75, 0x61, 0x79, 0x65, 0x32, 0x11,
				0x63, 0x61, 0x6c, 0x65, 0x62, 0x40, 0x6d, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x63, 0x6f,
				0x6d, 0x40, 0x01,
			},
			output: `{"firstName":"caleb","lastName":"quaye","email":"caleb@myspace.com","device":1}`,
		},
		{
			name:       "any: protobuf to json 1",
			message:    "testing.Envelope",
			importPath: "../../../config/test/protobuf/schema",
			input: []byte{
				0x8, 0xeb, 0x5, 0x12, 0x2b, 0xa, 0x22, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
				0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
				0x67, 0x2e, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e, 0x12, 0x5, 0xa, 0x3, 0x62, 0x6f, 0x62,
			},
			output: `{"id":747,"content":{"@type":"type.googleapis.com/testing.Person","firstName":"bob"}}`,
		},
		{
			name:       "any: protobuf to json 2",
			message:    "testing.Envelope",
			importPath: "../../../config/test/protobuf/schema",
			input: []byte{
				0x8, 0xeb, 0x5, 0x12, 0x2a, 0xa, 0x21, 0x74, 0x79, 0x70, 0x65, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
				0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e,
				0x67, 0x2e, 0x48, 0x6f, 0x75, 0x73, 0x65, 0x12, 0x5, 0x12, 0x3, 0x31, 0x32, 0x33,
			},
			output: `{"id":747,"content":{"@type":"type.googleapis.com/testing.House","address":"123"}}`,
		},
	}

	for i, test := range tests {
		t.Run(test.name+"/"+strconv.Itoa(i), func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: to_json
message: %v
import_paths: [ %v ]
use_proto_names: %t
use_enum_numbers: %t
`, test.message, test.importPath, test.useProtoNames, test.useEnumNumbers), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(t.Context(), service.NewMessage(test.input))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(mBytes))
			require.NoError(t, msgs[0].GetError())
		})

		t.Run(test.name+" bsr", func(t *testing.T) {
			mockBSRServerAddress := runMockBSRServer(t, test.importPath)

			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: to_json
message: %v
bsr:
  - module: "testing"
    url: %s
use_proto_names: %t
use_enum_numbers: %t
`, test.message, "http://"+mockBSRServerAddress, test.useProtoNames, test.useEnumNumbers), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			msgs, res := proc.Process(t.Context(), service.NewMessage(test.input))
			require.NoError(t, res)
			require.Len(t, msgs, 1)

			mBytes, err := msgs[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(mBytes))
			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestProtobufErrors(t *testing.T) {
	type testCase struct {
		name       string
		operator   string
		message    string
		importPath string
		input      string
		output     string
	}

	tests := []testCase{
		{
			name:       "json to protobuf unknown field",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      `{"firstName":"john","lastName":"oates","ageFoo":10}`,
			output:     "unknown field \"ageFoo\"",
		},
		{
			name:       "json to protobuf invalid value",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      `not valid json`,
			output:     "syntax error (line 1:1): invalid value not",
		},
		{
			name:       "json to protobuf invalid string",
			operator:   "from_json",
			message:    "testing.Person",
			importPath: "../../../config/test/protobuf/schema",
			input:      `{"firstName":5,"lastName":"quaye","email":"caleb@myspace.com"}`,
			output:     "invalid value for string field firstName: 5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := protobufProcessorSpec().ParseYAML(fmt.Sprintf(`
operator: %v
message: %v
import_paths: [ %v ]
`, test.operator, test.message, test.importPath), nil)
			require.NoError(t, err)

			proc, err := newProtobuf(conf, service.MockResources())
			require.NoError(t, err)

			_, err = proc.Process(t.Context(), service.NewMessage([]byte(test.input)))
			require.Error(t, err)
			require.Contains(t, err.Error(), test.output)
		})
	}
}

func TestProcessorConfigLinting(t *testing.T) {
	type testCase struct {
		name        string
		input       string
		errContains string
	}

	testCases := []testCase{
		{
			name: "valid import_paths config",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
  import_paths: [ ./mypath ]
`,
		},
		{
			name: "valid bsr config",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
  bsr:
    - module: "testing"
`,
		},
		{
			name: "can't set both import_paths and bsr",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
  import_paths: [ ./mypath ]
  bsr:
    - module: "buf.build/exampleco/mymodule"
`,
			errContains: "both `import_paths` and `bsr` can't be set simultaneously",
		},
		{
			name: "require one of import_paths and bsr",
			input: `
protobuf:
  operator: to_json
  message: testing.Person
`,
			errContains: "at least one of `import_paths`and `bsr` must be set",
		},
	}
	env := service.NewEnvironment()
	for _, test := range testCases {
		t.Run(test.name, func(_ *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.input)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

type fileDescriptorSetServer struct {
	fileDescriptorSet *descriptorpb.FileDescriptorSet
}

func (s *fileDescriptorSetServer) GetFileDescriptorSet(_ context.Context, request *connect.Request[v1beta1.GetFileDescriptorSetRequest]) (*connect.Response[v1beta1.GetFileDescriptorSetResponse], error) {
	resp := &v1beta1.GetFileDescriptorSetResponse{FileDescriptorSet: s.fileDescriptorSet, Version: request.Msg.GetVersion()}
	return connect.NewResponse(resp), nil
}

func runMockBSRServer(t *testing.T, importPath string) string {
	// load files into protoregistry.Files
	mockResources := service.MockResources()
	files, _, err := loadDescriptors(mockResources.FS(), []string{importPath})
	require.NoError(t, err)

	// populate into a FileDescriptorSet
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
	standardImportPaths := make(map[string]bool)
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		fileDescriptorSet.File = append(fileDescriptorSet.File, protodesc.ToFileDescriptorProto(fd))
		// find any standard imports used https://protobuf.com/docs/descriptors#standard-imports
		for i := 0; i < fd.Imports().Len(); i++ {
			imp := fd.Imports().Get(i)
			if strings.HasPrefix(imp.Path(), "google/protobuf/") {
				standardImportPaths[imp.Path()] = true
			}
		}
		return true
	})

	// add standard imports to the FileDescriptorSet
	for standardImportPath := range standardImportPaths {
		fd, err := protoregistry.GlobalFiles.FindFileByPath(standardImportPath)
		require.NoError(t, err)
		fileDescriptorSet.File = append(fileDescriptorSet.File, protodesc.ToFileDescriptorProto(fd))
	}

	// run GRPC server on an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	fileDescriptorSetServer := &fileDescriptorSetServer{fileDescriptorSet: fileDescriptorSet}
	mux.Handle(reflectv1beta1connect.NewFileDescriptorSetServiceHandler(fileDescriptorSetServer))
	go func() {
		if err := http.Serve(listener, h2c.NewHandler(mux, &http2.Server{})); err != nil && !errors.Is(err, http.ErrServerClosed) {
			require.NoError(t, err)
		}
	}()

	return listener.Addr().String()
}
