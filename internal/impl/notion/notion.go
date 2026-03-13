// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package notion

import (
	"net/http"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
)

const (
	nrFieldAPIKey        = "api_key"
	nrFieldNotionVersion = "notion_version"
)

func notionResourceFields() []*service.ConfigField {
	fields := []*service.ConfigField{
		service.NewStringField(nrFieldAPIKey).
			Description("Notion API key used for authentication."),
		service.NewStringField(nrFieldNotionVersion).
			Description("Notion API version.").
			Default("2022-06-28"),
	}
	return append(fields, httpclient.Fields("https://api.notion.com")...)
}

// notionClient is returned by the notion resource. Processors receive this
// via mgr.AccessResource and use HTTP for requests and BaseURL for URL construction.
type notionClient struct {
	client  *http.Client
	baseURL string
}

// notionVersionTransport sets the Notion-Version header on every request and
// delegates to the wrapped RoundTripper.
type notionVersionTransport struct {
	inner         http.RoundTripper
	notionVersion string
}

func (t *notionVersionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Notion-Version", t.notionVersion)
	return t.inner.RoundTrip(req)
}

func init() {
	if err := service.GlobalEnvironment().RegisterResource("notion", notionResourceFields(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (any, error) {
			apiKey, err := conf.FieldString(nrFieldAPIKey)
			if err != nil {
				return nil, err
			}

			notionVersion, err := conf.FieldString(nrFieldNotionVersion)
			if err != nil {
				return nil, err
			}

			baseURL, err := httpclient.BaseURLFromParsed(conf)
			if err != nil {
				return nil, err
			}

			cfg, err := httpclient.NewConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			cfg.AuthSigner = httpclient.BearerTokenSigner(apiKey)

			client, err := httpclient.NewClient(cfg, mgr)
			if err != nil {
				return nil, err
			}
			client.Transport = &notionVersionTransport{
				inner:         client.Transport,
				notionVersion: notionVersion,
			}

			return notionClient{
				client:  client,
				baseURL: baseURL,
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
