// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
)

// Field names shared by all Salesforce inputs.
const (
	sfFieldOrgURL       = "org_url"
	sfFieldClientID     = "client_id"
	sfFieldClientSecret = "client_secret"
	sfFieldAPIVersion   = "api_version"

	sfFieldBatching = "batching"

	sfFieldCheckpointCache    = "checkpoint_cache"
	sfFieldCheckpointCacheKey = "checkpoint_cache_key"
	sfFieldCheckpointLimit    = "checkpoint_limit"

	sfFieldGRPC                     = "grpc"
	sfFieldGRPCReconnectBaseDelay   = "reconnect_base_delay"
	sfFieldGRPCReconnectMaxDelay    = "reconnect_max_delay"
	sfFieldGRPCReconnectMaxAttempts = "reconnect_max_attempts"
	sfFieldGRPCShutdownTimeout      = "shutdown_timeout"
	sfFieldGRPCBufferSize           = "buffer_size"

	sfFieldHTTP = "http"
)

// AuthConfig holds Salesforce OAuth 2.0 Client Credentials flow parameters.
type AuthConfig struct {
	OrgURL       string
	ClientID     string
	ClientSecret string
	APIVersion   string
}

// NewAuthConfigFromParsed parses an AuthConfig from a benthos parsed config.
func NewAuthConfigFromParsed(pConf *service.ParsedConfig) (AuthConfig, error) {
	var cfg AuthConfig
	var err error
	if cfg.OrgURL, err = pConf.FieldString(sfFieldOrgURL); err != nil {
		return cfg, err
	}
	if cfg.ClientID, err = pConf.FieldString(sfFieldClientID); err != nil {
		return cfg, err
	}
	if cfg.ClientSecret, err = pConf.FieldString(sfFieldClientSecret); err != nil {
		return cfg, err
	}
	if cfg.APIVersion, err = pConf.FieldString(sfFieldAPIVersion); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// authFieldSpecs returns the four auth field specs shared by every Salesforce input.
func authFieldSpecs() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(sfFieldOrgURL).
			Description("Salesforce instance base URL for your org, protocol included and no trailing slash. Used as the base for both the OAuth token endpoint and REST queries. Production orgs use `https://{my-domain}.my.salesforce.com`; sandboxes use `https://{my-domain}.sandbox.my.salesforce.com`. Legacy instance URLs (`https://na123.salesforce.com`) still work but My Domain URLs are strongly recommended by Salesforce.").
			Example("https://acme.my.salesforce.com").
			Example("https://acme--staging.sandbox.my.salesforce.com"),
		service.NewStringField(sfFieldClientID).
			Description("Consumer Key of the Salesforce Connected App authorized for the OAuth Client Credentials flow. Create the Connected App under Setup → App Manager → New Connected App, enable OAuth settings, enable the Client Credentials Flow under `Flow Enablement`, then copy the Consumer Key from `Manage Consumer Details`."),
		service.NewStringField(sfFieldClientSecret).
			Description("Consumer Secret of the Salesforce Connected App, paired with `client_id`. Sensitive — prefer environment variable interpolation (`${SALESFORCE_CLIENT_SECRET}`) over inlining.").
			Secret(),
		service.NewStringField(sfFieldAPIVersion).
			Description("Salesforce REST API version to target, prefixed with `v`. Affects endpoint paths (`/services/data/{api_version}/...`) and available fields/objects. Must be supported by your org — check Setup → Company Information. Older versions may lack recent fields.").
			Default("v65.0").
			Example("v65.0").
			Example("v62.0"),
	}
}

// CheckpointConfig holds cache-resource–backed checkpoint state.
type CheckpointConfig struct {
	Cache    string
	CacheKey string
	Limit    int
}

// NewCheckpointConfigFromParsed parses a CheckpointConfig from a benthos parsed config.
func NewCheckpointConfigFromParsed(pConf *service.ParsedConfig) (CheckpointConfig, error) {
	var cfg CheckpointConfig
	var err error
	if cfg.Cache, err = pConf.FieldString(sfFieldCheckpointCache); err != nil {
		return cfg, err
	}
	if cfg.CacheKey, err = pConf.FieldString(sfFieldCheckpointCacheKey); err != nil {
		return cfg, err
	}
	if cfg.Limit, err = pConf.FieldInt(sfFieldCheckpointLimit); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// httpFieldSpec returns the nested `http` object field populated from
// httpclient.Fields. The base_url field is included so httpclient.NewConfigFromParsed
// can parse the namespace cleanly; its value is overridden with org_url at
// construction time (see newHTTPConfigFromParsed).
func httpFieldSpec() *service.ConfigField {
	return service.NewObjectField(sfFieldHTTP, httpclient.Fields()...).
		Description("HTTP client configuration for Salesforce REST calls (OAuth token endpoint and, where applicable, data queries).").
		Advanced()
}

// newHTTPConfigFromParsed builds an httpclient.Config from the http namespace
// and uses the user-supplied orgURL as the base URL.
func newHTTPConfigFromParsed(orgURL string, pConf *service.ParsedConfig) (httpclient.Config, error) {
	cfg, err := httpclient.NewConfigFromParsedNoBaseURL(pConf.Namespace(sfFieldHTTP))
	if err != nil {
		return httpclient.Config{}, err
	}
	cfg.BaseURL = orgURL
	cfg.MetricPrefix = "salesforce_http"
	return cfg, nil
}

// GRPCConfig holds gRPC transport tuning for the Pub/Sub API connection.
type GRPCConfig struct {
	ReconnectBaseDelay   time.Duration
	ReconnectMaxDelay    time.Duration
	ReconnectMaxAttempts int
	ShutdownTimeout      time.Duration
	BufferSize           int
}

// NewGRPCConfigFromParsed parses a GRPCConfig from an already-namespaced parsed config.
func NewGRPCConfigFromParsed(pConf *service.ParsedConfig) (GRPCConfig, error) {
	var cfg GRPCConfig
	var err error
	if cfg.ReconnectBaseDelay, err = pConf.FieldDuration(sfFieldGRPCReconnectBaseDelay); err != nil {
		return cfg, err
	}
	if cfg.ReconnectMaxDelay, err = pConf.FieldDuration(sfFieldGRPCReconnectMaxDelay); err != nil {
		return cfg, err
	}
	if cfg.ReconnectMaxAttempts, err = pConf.FieldInt(sfFieldGRPCReconnectMaxAttempts); err != nil {
		return cfg, err
	}
	if cfg.ShutdownTimeout, err = pConf.FieldDuration(sfFieldGRPCShutdownTimeout); err != nil {
		return cfg, err
	}
	if cfg.BufferSize, err = pConf.FieldInt(sfFieldGRPCBufferSize); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// grpcFieldSpec returns the nested `grpc` object field. Shared between salesforce_cdc
// and salesforce_platform_events inputs.
func grpcFieldSpec() *service.ConfigField {
	return service.NewObjectField(sfFieldGRPC,
		service.NewDurationField(sfFieldGRPCReconnectBaseDelay).
			Description("Base delay for gRPC reconnection backoff.").
			Default("500ms"),
		service.NewDurationField(sfFieldGRPCReconnectMaxDelay).
			Description("Maximum delay for gRPC reconnection backoff.").
			Default("30s"),
		service.NewIntField(sfFieldGRPCReconnectMaxAttempts).
			Description("Maximum number of gRPC reconnection attempts. 0 means unlimited.").
			Default(0),
		service.NewDurationField(sfFieldGRPCShutdownTimeout).
			Description("Timeout for graceful gRPC client shutdown.").
			Default("10s"),
		service.NewIntField(sfFieldGRPCBufferSize).
			Description("Size of the internal gRPC event receive buffer.").
			Default(1000),
	).Description("gRPC transport tuning for the Pub/Sub API connection.").Advanced()
}
