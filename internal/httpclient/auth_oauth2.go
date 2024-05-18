package httpclient

import (
	"context"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/benthosdev/benthos/v4/public/service"
)

const aFieldOAuth2 = "oauth2"

// AuthFieldSpecsExpanded includes OAuth2 and JWT fields that might not be
// appropriate for all components.
func AuthFieldSpecsExpanded() []*service.ConfigField {
	pubAuthFields := service.NewHTTPRequestAuthSignerFields()
	splicedFields := []*service.ConfigField{
		pubAuthFields[0],
		oAuth2FieldSpec(),
	}
	return append(splicedFields, pubAuthFields[1:]...)
}

type oauth2Config struct {
	Enabled        bool
	ClientKey      string
	ClientSecret   string
	TokenURL       string
	Scopes         []string
	EndpointParams map[string][]string
}

// Client returns an http.Client with OAuth2 configured.
func (oauth oauth2Config) Client(ctx context.Context, base *http.Client) *http.Client {
	if !oauth.Enabled {
		return base
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

func oAuth2FieldSpec() *service.ConfigField {
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

func oauth2ClientCtorFromParsed(conf *service.ParsedConfig) (res func(context.Context, *http.Client) *http.Client, err error) {
	if !conf.Contains(aFieldOAuth2) {
		return
	}
	conf = conf.Namespace(aFieldOAuth2)

	var oldConf oauth2Config
	if oldConf.Enabled, err = conf.FieldBool(ao2FieldEnabled); err != nil {
		return
	}
	if oldConf.ClientKey, err = conf.FieldString(ao2FieldClientKey); err != nil {
		return
	}
	if oldConf.ClientSecret, err = conf.FieldString(ao2FieldClientSecret); err != nil {
		return
	}
	if oldConf.TokenURL, err = conf.FieldString(ao2FieldTokenURL); err != nil {
		return
	}
	if oldConf.Scopes, err = conf.FieldStringList(ao2FieldScopes); err != nil {
		return
	}
	var endpointParams map[string]*service.ParsedConfig
	if endpointParams, err = conf.FieldAnyMap(ao2FieldEndpointParams); err != nil {
		return
	}
	oldConf.EndpointParams = map[string][]string{}
	for k, v := range endpointParams {
		if oldConf.EndpointParams[k], err = v.FieldStringList(); err != nil {
			return
		}
	}

	res = oldConf.Client
	return
}
