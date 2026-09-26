// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package esoutput provides config fields shared by the Elasticsearch and
// OpenSearch outputs.
package esoutput

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// URLsField returns the `urls` field listing the cluster nodes to connect to.
func URLsField() *service.ConfigField {
	return service.NewStringListField("urls").
		Description("A list of URLs to connect to. If an item in the list contains commas, it is split into multiple URLs.").
		Example([]string{"http://localhost:9200"})
}

// IndexField returns the `index` field, where product names the search engine.
func IndexField(product string) *service.ConfigField {
	return service.NewInterpolatedStringField("index").
		Description("The " + product + " index where messages are published.")
}

// ActionField returns the Elasticsearch `action` field.
func ActionField() *service.ConfigField {
	return service.NewInterpolatedStringField("action").
		Description("The action to perform on each document. This field must resolve to one of the following action types: `index`, `update`, `delete`, `create`, or `upsert`.\n\n" +
			"For more information on how the `update` action works, see the `Updating Documents` example. For examples of how to use the `create` and `upsert` actions, see the `Create Documents` and `Upserting Documents` examples.").
		ShortDescription("The action to take on the document: index, update, delete, create or upsert.")
}

// IDField returns the `id` field that sets the document ID of each message.
func IDField() *service.ConfigField {
	return service.NewInterpolatedStringField("id").
		Description(`The ID for indexed messages. Use xref:configuration:interpolation.adoc#bloblang-queries[function interpolations] to dynamically create a unique ID for each message.`).
		Example(`${!counter()}-${!timestamp_unix()}`)
}

// PipelineField returns the `pipeline` field naming an ingest pipeline.
func PipelineField() *service.ConfigField {
	return service.NewInterpolatedStringField("pipeline").
		Description("The ID of an optional ingest pipeline to preprocess incoming documents before they are indexed.").
		Advanced().
		Default("")
}

// RoutingField returns the `routing` field that sets the shard routing value of
// each document.
func RoutingField() *service.ConfigField {
	return service.NewInterpolatedStringField("routing").
		Description("A custom routing value for each document, which determines the shard it is stored on. When empty, the document ID is used.").
		Advanced().
		Default("")
}

// RetryOnConflictField returns the Elasticsearch `retry_on_conflict` field.
func RetryOnConflictField() *service.ConfigField {
	return service.NewIntField("retry_on_conflict").
		Description("The number of times to retry an update operation when a version conflict occurs.").
		Advanced().
		Default(0)
}

// APIKeyField returns the Elasticsearch `api_key` field.
func APIKeyField() *service.ConfigField {
	return service.NewStringField("api_key").
		Description("An API key to authenticate with. If set, it supersedes basic authentication.").
		Default("").Secret()
}

// BasicAuthField returns the `basic_auth` object field, where product names the
// search engine.
func BasicAuthField(product string) *service.ConfigField {
	return service.NewObjectField("basic_auth",
		service.NewBoolField("enabled").
			Description("Whether to send the `username` and `password` credentials with each request.").
			Default(false),
		service.NewStringField("username").
			Description("A username to authenticate as.").
			Default(""),
		service.NewStringField("password").
			Description("A password to authenticate with.").
			Default("").Secret(),
	).Description("Configure basic authentication credentials for connecting to " + product + ". When enabled, these credentials are sent with each request to authenticate with the cluster.").
		Advanced().
		Optional()
}
