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

package client

// Transcoder represents the transcoder that will be used by Couchbase.
type Transcoder string

const (
	// TranscoderRaw raw operation.
	TranscoderRaw Transcoder = "raw"
	// TranscoderRawJSON rawjson transcoder.
	TranscoderRawJSON Transcoder = "rawjson"
	// TranscoderRawString rawstring transcoder.
	TranscoderRawString Transcoder = "rawstring"
	// TranscoderJSON JSON transcoder.
	TranscoderJSON Transcoder = "json"
	// TranscoderLegacy Legacy transcoder.
	TranscoderLegacy Transcoder = "legacy"
)

// Operation represents the operation that will be performed by Couchbase.
type Operation string

const (
	// OperationGet Get operation.
	OperationGet Operation = "get"
	// OperationInsert Insert operation.
	OperationInsert Operation = "insert"
	// OperationRemove Delete operation.
	OperationRemove Operation = "remove"
	// OperationReplace Replace operation.
	OperationReplace Operation = "replace"
	// OperationUpsert Upsert operation.
	OperationUpsert Operation = "upsert"
)
