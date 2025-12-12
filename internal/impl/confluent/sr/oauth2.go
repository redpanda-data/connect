// Copyright 2025 Redpanda Data, Inc.
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

package sr

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const aFieldOAuth2 = "oauth2"

type oauth2Config struct {
	Enabled        bool
	ClientKey      string
	ClientSecret   string
	TokenURL       string
	Scopes         []string
	EndpointParams map[string][]string
}

func (oauth oauth2Config) wrapClient(ctx context.Context, base *http.Client) *http.Client {
	if !oauth.Enabled {
		return base
	}

	// Support for refresh_token grant type with bootstrapped refresh token to obtain access token
	if gt, ok := oauth.EndpointParams["grant_type"]; ok && gt[0] == "refresh_token" {
		conf := &oauth2.Config{
			ClientID:     oauth.ClientKey,
			ClientSecret: oauth.ClientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL:  oauth.TokenURL,
				AuthStyle: oauth2.AuthStyleAutoDetect,
			},
			Scopes: oauth.Scopes,
		}
		// We don't consider bootstrapped access token if any as it might be expired, rather we generate a new one
		token := &oauth2.Token{}
		if rt, ok := oauth.EndpointParams["refresh_token"]; ok {
			token.RefreshToken = rt[0]
		}
		return conf.Client(context.WithValue(ctx, oauth2.HTTPClient, base), token)
	}

	conf := &clientcredentials.Config{
		ClientID:       oauth.ClientKey,
		ClientSecret:   oauth.ClientSecret,
		TokenURL:       oauth.TokenURL,
		Scopes:         oauth.Scopes,
		EndpointParams: oauth.EndpointParams,
	}

	return conf.Client(context.WithValue(ctx, oauth2.HTTPClient, base))
}

//------------------------------------------------------------------------------

const (
	ao2FieldEnabled        = "enabled"
	ao2FieldClientKey      = "client_key"
	ao2FieldClientSecret   = "client_secret"
	ao2FieldTokenURL       = "token_url"
	ao2FieldScopes         = "scopes"
	ao2FieldEndpointParams = "endpoint_params"
)

// OAuth2FieldSpec returns the configuration field that can be used with [OAuth2ClientOptFromConfig].
func OAuth2FieldSpec() *service.ConfigField {
	return service.NewObjectField(aFieldOAuth2,
		service.NewBoolField(ao2FieldEnabled).
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),

		service.NewStringField(ao2FieldClientKey).
			Description("A value used to identify the client to the token provider.").
			Default(""),

		service.NewStringField(ao2FieldClientSecret).
			Description("A secret used to establish ownership of the client key.").
			Default("").Secret(),

		service.NewURLField(ao2FieldTokenURL).
			Description("The URL of the token provider.").
			Default(""),

		service.NewStringListField(ao2FieldScopes).
			Description("A list of optional requested permissions.").
			Default([]any{}).
			Advanced().
			Version("3.45.0"),

		service.NewAnyMapField(ao2FieldEndpointParams).
			Description("A list of optional endpoint parameters, values should be arrays of strings.").
			Advanced().
			Example(map[string]any{
				"foo": []string{"meow", "quack"},
				"bar": []string{"woof"},
			}).
			Default(map[string]any{}).
			Version("4.21.0").
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

// OAuth2ClientOptFromConfig returns the extracted options, a cancel function to cancel the oauth refresh
func OAuth2ClientOptFromConfig(conf *service.ParsedConfig) (opts []sr.ClientOpt, cancel context.CancelFunc, err error) {
	cancel = func() {}
	if !conf.Contains(aFieldOAuth2) {
		return
	}
	conf = conf.Namespace(aFieldOAuth2)

	var oConf oauth2Config
	if oConf.Enabled, err = conf.FieldBool(ao2FieldEnabled); err != nil {
		return
	}
	if oConf.ClientKey, err = conf.FieldString(ao2FieldClientKey); err != nil {
		return
	}
	if oConf.ClientSecret, err = conf.FieldString(ao2FieldClientSecret); err != nil {
		return
	}
	if oConf.TokenURL, err = conf.FieldString(ao2FieldTokenURL); err != nil {
		return
	}
	if oConf.Scopes, err = conf.FieldStringList(ao2FieldScopes); err != nil {
		return
	}
	var endpointParams map[string]*service.ParsedConfig
	if endpointParams, err = conf.FieldAnyMap(ao2FieldEndpointParams); err != nil {
		return
	}
	oConf.EndpointParams = map[string][]string{}
	for k, v := range endpointParams {
		if oConf.EndpointParams[k], err = v.FieldStringList(); err != nil {
			return
		}
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	cl = oConf.wrapClient(ctx, cl)
	return []sr.ClientOpt{sr.HTTPClient(cl)}, cancel, nil
}
