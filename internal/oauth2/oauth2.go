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

package oauth2

import (
	"context"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldEnabled        = "enabled"
	fieldClientKey      = "client_key"
	fieldClientSecret   = "client_secret"
	fieldTokenURL       = "token_url"
	fieldScopes         = "scopes"
	fieldEndpointParams = "endpoint_params"
)

// Config holds OAuth2 authentication configuration.
type Config struct {
	Enabled        bool
	ClientKey      string
	ClientSecret   string
	TokenURL       string
	Scopes         []string
	EndpointParams map[string][]string
}

// FieldSpec returns the configuration spec for OAuth2 authentication.
func FieldSpec() *service.ConfigField {
	return service.NewObjectField("oauth2",
		service.NewBoolField(fieldEnabled).
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),

		service.NewStringField(fieldClientKey).
			Description("A value used to identify the client to the token provider.").
			Default(""),

		service.NewStringField(fieldClientSecret).
			Description("A secret used to establish ownership of the client key.").
			Default("").Secret(),

		service.NewURLField(fieldTokenURL).
			Description("The URL of the token provider.").
			Default(""),

		service.NewStringListField(fieldScopes).
			Description("A list of optional requested permissions.").
			Default([]any{}).
			Advanced(),

		service.NewAnyMapField(fieldEndpointParams).
			Description("A list of optional endpoint parameters, values should be arrays of strings.").
			Advanced().
			Example(map[string]any{
				"audience": []string{"https://example.com"},
				"resource": []string{"https://api.example.com"},
			}).
			Default(map[string]any{}).
			Optional().
			LintRule(`
root = if this.type() == "object" {
  this.values().map_each(ele -> if ele.type() != "array" {
    "field must be an object containing arrays of strings, got %s (%v)".format(ele.format_json(no_indent: true), ele.type())
  } else {
    ele.map_each(str -> if str.type() != "string" {
      "field values must be strings, got %s (%v)".format(str.format_json(no_indent: true), str.type())
    } else { deleted() })
  }).
    flatten()
}
`),
	).
		Description("Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.").
		Optional().Advanced()
}

// ParseConfig parses OAuth2 configuration from a parsed config.
func ParseConfig(pConf *service.ParsedConfig) (Config, error) {
	var conf Config
	var err error

	if conf.Enabled, err = pConf.FieldBool(fieldEnabled); err != nil {
		return conf, err
	}

	if !conf.Enabled {
		return conf, nil
	}

	if conf.ClientKey, err = pConf.FieldString(fieldClientKey); err != nil {
		return conf, err
	}
	if conf.ClientSecret, err = pConf.FieldString(fieldClientSecret); err != nil {
		return conf, err
	}
	if conf.TokenURL, err = pConf.FieldString(fieldTokenURL); err != nil {
		return conf, err
	}
	if conf.Scopes, err = pConf.FieldStringList(fieldScopes); err != nil {
		return conf, err
	}

	var endpointParams map[string]*service.ParsedConfig
	if endpointParams, err = pConf.FieldAnyMap(fieldEndpointParams); err != nil {
		return conf, err
	}
	conf.EndpointParams = make(map[string][]string, len(endpointParams))
	for k, v := range endpointParams {
		if conf.EndpointParams[k], err = v.FieldStringList(); err != nil {
			return conf, err
		}
	}

	return conf, nil
}

// TokenSource returns an oauth2.TokenSource for the configuration.
func (c Config) TokenSource(ctx context.Context) oauth2.TokenSource {
	if !c.Enabled {
		return nil
	}

	// Support for refresh_token grant type with bootstrapped refresh token to obtain access token
	if gt, ok := c.EndpointParams["grant_type"]; ok && len(gt) > 0 && gt[0] == "refresh_token" {
		conf := &oauth2.Config{
			ClientID:     c.ClientKey,
			ClientSecret: c.ClientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL:  c.TokenURL,
				AuthStyle: oauth2.AuthStyleAutoDetect,
			},
			Scopes: c.Scopes,
		}

		// We don't consider bootstrapped access token if any as it might be
		// expired, rather we generate a new one
		token := new(oauth2.Token)
		if rt, ok := c.EndpointParams["refresh_token"]; ok && len(rt) > 0 {
			token.RefreshToken = rt[0]
		}
		return conf.TokenSource(ctx, token)
	}

	conf := &clientcredentials.Config{
		ClientID:       c.ClientKey,
		ClientSecret:   c.ClientSecret,
		TokenURL:       c.TokenURL,
		Scopes:         c.Scopes,
		EndpointParams: c.EndpointParams,
	}
	return conf.TokenSource(ctx)
}

// HTTPClient returns an http.Client with OAuth2 configured. This wraps the
// TokenSource in an HTTP transport.
func (c Config) HTTPClient(ctx context.Context, base *http.Client) (*http.Client, error) {
	if !c.Enabled {
		return base, nil
	}

	return oauth2.NewClient(context.WithValue(ctx, oauth2.HTTPClient, base), c.TokenSource(ctx)), nil
}
