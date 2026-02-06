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

// Package salesforce provides a Benthos salesforceProcessor that integrates with the Salesforce APIs
// to fetch data based on input messages. It allows querying Salesforce resources
// such as .... TODO

package salesforce

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// salesforceProcessor is the Benthos salesforceProcessor implementation for Salesforce queries.
// It holds the client state and orchestrates calls into the salesforcehttp package.
type salesforceProcessor struct {
	log    *service.Logger
	client *salesforcehttp.Client
}

// SObjectList is the response from all the available sObjects
type SObjectList struct {
	Encoding     string    `json:"encoding"`
	MaxBatchSize int       `json:"maxBatchSize"`
	Sobjects     []SObject `json:"sobjects"`
}

// SObject is the minimal representation of an sObject
type SObject struct {
	Name string `json:"name"`
}

// DescribeResult sObject result
type DescribeResult struct {
	Fields []struct {
		Name string `json:"name"`
	} `json:"fields"`
}

// QueryResult of the salesforce search query
type QueryResult struct {
	TotalSize int  `json:"totalSize"`
	Done      bool `json:"done"`
}

// newSalesforceProcessorConfigSpec creates a new Configuration specification for the Salesforce processor
func newSalesforceProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Fetches data from Salesforce based on input messages").
		Description(`This salesforceProcessor takes input messages containing Salesforce queries and returns Salesforce data.

Supports the following Salesforce resources:
- todo

Configuration examples:

` + "```configYAML" + `
# Minimal configuration
pipeline:
  processors:
    - salesforce:
        org_url: "https://your-domain.salesforce.com"
        client_id: "${SALESFORCE_CLIENT_ID}"
        client_secret: "${SALESFORCE_CLIENT_SECRET}"

# Full configuration
pipeline:
  processors:
    - salesforce:
        org_url: "https://your-domain.salesforce.com"
        client_id: "${SALESFORCE_CLIENT_ID}"
        client_secret: "${SALESFORCE_CLIENT_SECRET}"
		restapi_version: "v64.0"
        request_timeout: "30s"
        max_retries: 50
` + "```").
		Field(service.NewStringField("org_url").
			Description("Salesforce instance base URL (e.g., https://your-domain.salesforce.com)")).
		Field(service.NewStringField("client_id").
			Description("Client ID for the Salesforce Connected App")).
		Field(service.NewStringField("client_secret").
			Description("Client Secret for the Salesforce Connected App").
			Secret()).
		Field(service.NewStringField("restapi_version").
			Description("Salesforce REST API version to use (example: v64.0). Default: v65.0").
			Default("v65.0")).
		Field(service.NewDurationField("request_timeout").
			Description("HTTP request timeout").
			Default("30s")).
		Field(service.NewIntField("max_retries").
			Description("Maximum number of retries in case of 429 HTTP Status Code").
			Default(10))
}

func newSalesforceProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*salesforceProcessor, error) {
	orgURL, err := conf.FieldString("org_url")
	if err != nil {
		return nil, err
	}

	if _, err := url.ParseRequestURI(orgURL); err != nil {
		return nil, errors.New("org_url is not a valid URL")
	}

	clientId, err := conf.FieldString("client_id")
	if err != nil {
		return nil, err
	}

	clientSecret, err := conf.FieldString("client_secret")
	if err != nil {
		return nil, err
	}

	apiVersion, err := conf.FieldString("restapi_version")
	if err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration("request_timeout")
	if err != nil {
		return nil, err
	}

	maxRetries, err := conf.FieldInt("max_retries")
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{Timeout: timeout}

	salesforceHttp, err := salesforcehttp.NewClient(mgr.Logger(), orgURL, clientId, clientSecret, apiVersion, maxRetries, mgr.Metrics(), httpClient)
	if err != nil {
		return nil, err
	}

	return &salesforceProcessor{
		client: salesforceHttp,
		log:    mgr.Logger(),
	}, nil
}

func (s *salesforceProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var batch service.MessageBatch
	inputMsg, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	s.log.Debugf("Fetching from Salesforce.. Input: %s", string(inputMsg))

	res, err := s.client.GetAvailableResources(ctx)
	if err != nil {
		return nil, err
	}

	m := service.NewMessage(res)
	batch = append(batch, m)

	return batch, nil
}

func (*salesforceProcessor) Close(context.Context) error { return nil }

func init() {
	if err := service.RegisterProcessor(
		"salesforce", newSalesforceProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newSalesforceProcessor(conf, mgr)
		},
	); err != nil {
		panic(err)
	}
}
