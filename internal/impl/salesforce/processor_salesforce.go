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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcegrpc"
	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
)

// salesforceProcessor is the Benthos salesforceProcessor implementation for Salesforce queries.
// It holds the client state and orchestrates calls into the salesforcehttp package.
type salesforceProcessor struct {
	log         *service.Logger
	client      *salesforcehttp.Client
	res         *service.Resources
	binLogCache string
	req         Request

	// CDC configuration
	cdcEnabled      bool
	cdcObjects      []string
	cdcTopicName    string
	cdcBatchSize    int32
	cdcBufferSize   int32
	cdcReplayPreset string

	// Pub/Sub topic override (for Platform Events or arbitrary topics)
	pubsubTopic string

	// gRPC reconnection backoff settings
	grpcReconnectBaseDelay   time.Duration
	grpcReconnectMaxDelay    time.Duration
	grpcReconnectMaxAttempts int

	// gRPC shutdown timeout
	grpcShutdownTimeout time.Duration

	// gRPC client for CDC/Pub/Sub streaming (lazy-initialized)
	grpcClient *salesforcegrpc.Client

	// Number of SObjects to fetch in parallel during the REST snapshot
	parallelFetch int

	// dispatchMu serializes dispatch calls to prevent concurrent triggers from
	// executing the same fetch before the checkpoint is saved.
	dispatchMu sync.Mutex
}

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

# Full configuration with CDC
pipeline:
  processors:
    - salesforce:
        org_url: "https://your-domain.salesforce.com"
        client_id: "${SALESFORCE_CLIENT_ID}"
        client_secret: "${SALESFORCE_CLIENT_SECRET}"
        restapi_version: "v64.0"
        request_timeout: "30s"
        max_retries: 50
        cdc_enabled: true
        cdc_objects:
          - Account
          - Contact
        cdc_batch_size: 100
        cdc_buffer_size: 1000
        cdc_replay_preset: "latest"

# Platform Events (standalone, no REST snapshot)
pipeline:
  processors:
    - salesforce:
        org_url: "https://your-domain.salesforce.com"
        client_id: "${SALESFORCE_CLIENT_ID}"
        client_secret: "${SALESFORCE_CLIENT_SECRET}"
        pubsub_topic: "/event/MyEvent__e"
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
			Default(10)).
		// query_type selects which Salesforce API to use:
		//   "rest"    – REST API (default). Without a query, fetches all SObjects with checkpointing.
		//   "graphql" – GraphQL API. Requires a query to be provided.
		Field(service.NewStringField("query_type").
			Description("API mode: \"rest\" (default) or \"graphql\"").
			Default("rest")).
		// query is an optional SOQL or GraphQL query string.
		//   For rest:    a SOQL expression, e.g. "SELECT Id, Name FROM Account"
		//   For graphql: a GraphQL query string
		// When omitted the processor defaults to fetching all SObjects via REST with checkpointing.
		Field(service.NewStringField("query").
			Description("Optional SOQL (rest) or GraphQL query. When empty, all SObjects are fetched via REST.").
			Default("")).
		// CDC configuration fields
		Field(service.NewBoolField("cdc_enabled").
			Description("Enable Change Data Capture streaming after REST snapshot completes").
			Default(false)).
		Field(service.NewStringListField("cdc_objects").
			Description("SObject types to capture changes for (e.g., [\"Account\", \"Contact\"]). When empty, subscribes to /data/ChangeEvents for all objects.").
			Default([]any{})).
		Field(service.NewIntField("cdc_batch_size").
			Description("Number of CDC events to request per gRPC fetch").
			Default(100)).
		Field(service.NewIntField("cdc_buffer_size").
			Description("Size of the internal CDC event buffer").
			Default(1000)).
		Field(service.NewStringField("cdc_replay_preset").
			Description("CDC replay preset when no checkpoint exists: \"latest\" (default) or \"earliest\"").
			Default("latest")).
		// Pub/Sub topic override (for Platform Events or arbitrary topics)
		Field(service.NewStringField("pubsub_topic").
			Description("Arbitrary Pub/Sub API topic (e.g., \"/event/MyEvent__e\"). When set, overrides cdc_objects for topic selection.").
			Default("")).
		// gRPC reconnection backoff settings
		Field(service.NewDurationField("grpc_reconnect_base_delay").
			Description("Base delay for gRPC reconnection backoff").
			Default("500ms")).
		Field(service.NewDurationField("grpc_reconnect_max_delay").
			Description("Maximum delay for gRPC reconnection backoff").
			Default("30s")).
		Field(service.NewIntField("grpc_reconnect_max_attempts").
			Description("Maximum number of gRPC reconnection attempts (0 = unlimited)").
			Default(0)).
		// gRPC shutdown timeout
		Field(service.NewDurationField("grpc_shutdown_timeout").
			Description("Timeout for graceful gRPC client shutdown").
			Default("10s")).
		Field(service.NewStringField("cache_resource").
			Description("Name of the Benthos cache resource used for checkpointing state (must be defined in cache_resources).").
			Default("salesforce_checkpoint")).
		Field(service.NewIntField("parallel_fetch").
			Description("Number of SObjects to fetch concurrently during the REST snapshot (no-query mode). Higher values improve throughput but consume more API quota.").
			Default(1)).
		Field(service.NewIntField("query_batch_size").
			Description("Number of records Salesforce returns per query page (200–2000). Lower values reduce individual response size and avoid timeouts on wide SObjects.").
			Default(2000)).
		Field(service.NewStringListField("rest_objects").
			Description("Limit the REST snapshot to only these SObject types (e.g., [\"Account\", \"Contact\"]). When empty, all queryable SObjects are fetched.").
			Default([]any{}))
}

func newSalesforceProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*salesforceProcessor, error) {
	//if err := license.CheckRunningEnterprise(mgr); err != nil {
	//	return nil, err
	//}

	orgURL, err := conf.FieldString("org_url")
	if err != nil {
		return nil, err
	}

	if _, err := url.ParseRequestURI(orgURL); err != nil {
		return nil, errors.New("org_url is not a valid URL")
	}

	clientID, err := conf.FieldString("client_id")
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

	queryType, err := conf.FieldString("query_type")
	if err != nil {
		return nil, err
	}

	query, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}

	req, err := buildRequest(queryType, query)
	if err != nil {
		return nil, err
	}

	// Parse CDC configuration
	cdcEnabled, err := conf.FieldBool("cdc_enabled")
	if err != nil {
		return nil, err
	}

	cdcObjects, err := conf.FieldStringList("cdc_objects")
	if err != nil {
		return nil, err
	}

	cdcBatchSize, err := conf.FieldInt("cdc_batch_size")
	if err != nil {
		return nil, err
	}

	cdcBufferSize, err := conf.FieldInt("cdc_buffer_size")
	if err != nil {
		return nil, err
	}

	cdcReplayPreset, err := conf.FieldString("cdc_replay_preset")
	if err != nil {
		return nil, err
	}

	pubsubTopic, err := conf.FieldString("pubsub_topic")
	if err != nil {
		return nil, err
	}

	grpcReconnectBaseDelay, err := conf.FieldDuration("grpc_reconnect_base_delay")
	if err != nil {
		return nil, err
	}

	grpcReconnectMaxDelay, err := conf.FieldDuration("grpc_reconnect_max_delay")
	if err != nil {
		return nil, err
	}

	grpcReconnectMaxAttempts, err := conf.FieldInt("grpc_reconnect_max_attempts")
	if err != nil {
		return nil, err
	}

	grpcShutdownTimeout, err := conf.FieldDuration("grpc_shutdown_timeout")
	if err != nil {
		return nil, err
	}

	cacheResource, err := conf.FieldString("cache_resource")
	if err != nil {
		return nil, err
	}

	parallelFetch, err := conf.FieldInt("parallel_fetch")
	if err != nil {
		return nil, err
	}
	if parallelFetch < 1 {
		parallelFetch = 1
	}

	queryBatchSize, err := conf.FieldInt("query_batch_size")
	if err != nil {
		return nil, err
	}

	restObjects, err := conf.FieldStringList("rest_objects")
	if err != nil {
		return nil, err
	}

	// Build the CDC topic name: pubsub_topic takes priority, otherwise derive from cdc_objects
	cdcTopicName := pubsubTopic
	if cdcTopicName == "" {
		cdcTopicName = buildCDCTopicName(cdcObjects)
	}

	httpClient := &http.Client{Timeout: timeout}

	salesforceHttp, err := salesforcehttp.NewClient(orgURL, clientID, clientSecret, apiVersion, maxRetries, queryBatchSize, httpClient, mgr.Logger(), mgr.Metrics())
	if err != nil {
		return nil, err
	}

	if len(restObjects) > 0 {
		salesforceHttp.SetRestObjects(restObjects)
	}

	return &salesforceProcessor{
		client:                   salesforceHttp,
		log:                      mgr.Logger(),
		res:                      mgr,
		req:                      req,
		binLogCache:              cacheResource,
		cdcEnabled:               cdcEnabled,
		cdcObjects:               cdcObjects,
		cdcTopicName:             cdcTopicName,
		cdcBatchSize:             int32(cdcBatchSize),
		cdcBufferSize:            int32(cdcBufferSize),
		cdcReplayPreset:          cdcReplayPreset,
		pubsubTopic:              pubsubTopic,
		grpcReconnectBaseDelay:   grpcReconnectBaseDelay,
		grpcReconnectMaxDelay:    grpcReconnectMaxDelay,
		grpcReconnectMaxAttempts: grpcReconnectMaxAttempts,
		grpcShutdownTimeout:      grpcShutdownTimeout,
		parallelFetch:            parallelFetch,
	}, nil
}

// buildCDCTopicName constructs the Pub/Sub API topic name for CDC.
// For specific objects: /data/<Object>ChangeEvent (single object)
// For all objects: /data/ChangeEvents
func buildCDCTopicName(objects []string) string {
	if len(objects) == 0 {
		return "/data/ChangeEvents"
	}
	if len(objects) == 1 {
		return "/data/" + objects[0] + "ChangeEvent"
	}
	// Multiple specific objects: use the combined channel
	// Salesforce requires subscribing to individual channels or the catch-all
	// For simplicity, use catch-all when multiple objects are specified
	return "/data/ChangeEvents"
}

func buildRequest(queryType, query string) (Request, error) {
	var req Request

	switch queryType {
	case "rest":
		req.QueryType = QueryREST
	case "graphql":
		req.QueryType = QueryGraphQL
	default:
		return Request{}, fmt.Errorf("invalid query_type %q: must be \"rest\" or \"graphql\"", queryType)
	}

	if query != "" {
		req.Filter = FilterConfig{Enabled: true, Value: query}
	}

	return req, nil
}

func (s *salesforceProcessor) Process(ctx context.Context, _ *service.Message) (service.MessageBatch, error) {
	batch, err := s.Dispatch(ctx, s.req)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func (s *salesforceProcessor) Close(ctx context.Context) error {
	if s.grpcClient == nil {
		return nil
	}

	// Drain remaining events for final checkpoint
	remaining := s.grpcClient.DrainBuffer()
	if len(remaining) > 0 {
		var latestReplayID []byte
		for _, evt := range remaining {
			if len(evt.ReplayID) > 0 {
				latestReplayID = evt.ReplayID
			}
		}
		if len(latestReplayID) > 0 {
			state, err := s.loadState(ctx)
			if err == nil {
				if s.pubsubTopic != "" {
					state.PubSubReplayID = latestReplayID
					state.PubSubTopic = s.pubsubTopic
				} else {
					state.CDCReplayID = latestReplayID
				}
				if err := s.saveState(ctx, state); err != nil {
					s.log.Errorf("Failed to save final checkpoint: %v", err)
				} else {
					s.log.Infof("Final checkpoint saved (%d drained events)", len(remaining))
				}
			}
		}
	}

	return s.grpcClient.CloseWithTimeout(s.grpcShutdownTimeout)
}

// filterCDCEntity checks if a CDC event entity matches the configured objects.
// Used when subscribing to the catch-all /data/ChangeEvents topic to filter events.
func (s *salesforceProcessor) filterCDCEntity(entityName string) bool {
	if len(s.cdcObjects) == 0 {
		return true // No filter, accept all
	}
	for _, obj := range s.cdcObjects {
		if strings.EqualFold(obj, entityName) {
			return true
		}
	}
	return false
}
