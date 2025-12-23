// Copyright 2025 Redpanda Data, Inc.
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
package elasticsearch

import (
	"context"
	"net/http"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elasticsearch_v8 "github.com/elastic/go-elasticsearch/v8"
elasticsearch_v9 "github.com/elastic/go-elasticsearch/v9"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type BulkWriterBuilder interface {
	Bulk() BulkWriter
}

type BulkWriter interface {
	AddOpToBatch(
		batch service.MessageBatch,
		batchInterpolator *BatchInterpolator,
		batchIndex int,
		retryOnConflict int,
	) error

	Do(ctx context.Context) (result *Result, err error)
}

type Result struct {
	Errors  bool
	Results []error
	Took    time.Duration
}

type ElasticsearchConfig struct {
	Logger    elastictransport.Logger
	Transport http.RoundTripper

	Addresses []string
	Username  string
	Password  string
}

func (c *ElasticsearchConfig) ToV8Configuration() elasticsearch_v8.Config {
	return elasticsearch_v8.Config{
		Addresses: c.Addresses,
		Username:  c.Username,
		Password:  c.Password,
		Transport: c.Transport,
		Logger:    c.Logger,
	}
}

func (c *ElasticsearchConfig) ToV9Configuration() elasticsearch_v9.Config {
	return elasticsearch_v9.Config{
		Addresses: c.Addresses,
		Username:  c.Username,
		Password:  c.Password,
		Transport: c.Transport,
		Logger:    c.Logger,
	}
}


type BulkWriterConnector func(clientOpts ElasticsearchConfig) (BulkWriterBuilder, error)
