package httpclient

import (
	"context"
	"github.com/benthosdev/benthos/v4/internal/impl/aws"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"io/fs"
	"net/http"

	"github.com/benthosdev/benthos/v4/public/service"
)

// RequestSigner is a closure configured to enrich requests with various
// functions, usually authentication.
type RequestSigner func(f fs.FS, req *http.Request) error

const (
	aFieldBasicAuth = "basic_auth"
	aFieldOAuth     = "oauth"
	aFieldOAuth2    = "oauth2"
	aFieldJWT       = "jwt"
)

// AuthFieldSpecs returns a map of field specs for an auth type.
func AuthFieldSpecs() []*service.ConfigField {
	return []*service.ConfigField{
		oAuthFieldSpec(),
		BasicAuthField(),
		jwtFieldSpec(),
	}
}

// AuthFieldSpecsExpanded includes OAuth2 and JWT fields that might not be
// appropriate for all components.
func AuthFieldSpecsExpanded() []*service.ConfigField {
	return []*service.ConfigField{
		oAuthFieldSpec(),
		oAuth2FieldSpec(),
		BasicAuthField(),
		jwtFieldSpec(),
		awsV4FieldSpec(),
	}
}

// AuthSignerFromParsed takes a parsed config which is expected to contain
// fields from AuthFields, and returns a RequestSigner that implements the
// configured authentication strategies by enriching a request directly.
func AuthSignerFromParsed(conf *service.ParsedConfig) (RequestSigner, error) {
	oldConf, err := authConfFromParsed(conf)
	if err != nil {
		return nil, err
	}
	return oldConf.Sign, nil
}

func authConfFromParsed(conf *service.ParsedConfig) (oldConf AuthConfig, err error) {
	oldConf = NewAuthConfig()
	if oldConf.OAuth, err = oauthFromParsed(conf); err != nil {
		return
	}
	if oldConf.BasicAuth, err = basicAuthFromParsed(conf); err != nil {
		return
	}
	if oldConf.JWT, err = jwtAuthFromParsed(conf); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

const (
	abFieldEnabled  = "enabled"
	abFieldUsername = "username"
	abFieldPassword = "password"
)

// BasicAuthField returns a config field spec for basic authentication.
func BasicAuthField() *service.ConfigField {
	return service.NewObjectField(aFieldBasicAuth,
		service.NewBoolField(abFieldEnabled).
			Description("Whether to use basic authentication in requests.").
			Default(false),

		service.NewStringField(abFieldUsername).
			Description("A username to authenticate as.").
			Default(""),

		service.NewStringField(abFieldPassword).
			Description("A password to authenticate with.").
			Default("").Secret(),
	).Description("Allows you to specify basic authentication.").
		Advanced().
		Optional()
}

func basicAuthFromParsed(conf *service.ParsedConfig) (res BasicAuthConfig, err error) {
	res = NewBasicAuthConfig()
	if !conf.Contains(aFieldBasicAuth) {
		return
	}
	conf = conf.Namespace(aFieldBasicAuth)
	if res.Enabled, err = conf.FieldBool(abFieldEnabled); err != nil {
		return
	}
	if res.Username, err = conf.FieldString(abFieldUsername); err != nil {
		return
	}
	if res.Password, err = conf.FieldString(abFieldPassword); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

const (
	aoFieldEnabled           = "enabled"
	aoFieldConsumerKey       = "consumer_key"
	aoFieldConsumerSecret    = "consumer_secret"
	aoFieldAccessToken       = "access_token"
	aoFieldAccessTokenSecret = "access_token_secret"
)

func oAuthFieldSpec() *service.ConfigField {
	return service.NewObjectField(aFieldOAuth,
		service.NewBoolField(aoFieldEnabled).
			Description("Whether to use OAuth version 1 in requests.").
			Default(false),

		service.NewStringField(aoFieldConsumerKey).
			Description("A value used to identify the client to the service provider.").
			Default(""),

		service.NewStringField(aoFieldConsumerSecret).
			Description("A secret used to establish ownership of the consumer key.").
			Default("").Secret(),

		service.NewStringField(aoFieldAccessToken).
			Description("A value used to gain access to the protected resources on behalf of the user.").
			Default(""),

		service.NewStringField(aoFieldAccessTokenSecret).
			Description("A secret provided in order to establish ownership of a given access token.").
			Default("").Secret(),
	).
		Description("Allows you to specify open authentication via OAuth version 1.").
		Advanced().
		Optional()
}

func oauthFromParsed(conf *service.ParsedConfig) (res OAuthConfig, err error) {
	res = NewOAuthConfig()
	if !conf.Contains(aFieldOAuth) {
		return
	}
	conf = conf.Namespace(aFieldOAuth)
	if res.Enabled, err = conf.FieldBool(aoFieldEnabled); err != nil {
		return
	}
	if res.ConsumerKey, err = conf.FieldString(aoFieldConsumerKey); err != nil {
		return
	}
	if res.ConsumerSecret, err = conf.FieldString(aoFieldConsumerSecret); err != nil {
		return
	}
	if res.AccessToken, err = conf.FieldString(aoFieldAccessToken); err != nil {
		return
	}
	if res.AccessTokenSecret, err = conf.FieldString(aoFieldAccessTokenSecret); err != nil {
		return
	}
	return
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

func oauth2FromParsed(conf *service.ParsedConfig) (res OAuth2Config, err error) {
	res = NewOAuth2Config()
	if !conf.Contains(aFieldOAuth2) {
		return
	}
	conf = conf.Namespace(aFieldOAuth2)
	if res.Enabled, err = conf.FieldBool(ao2FieldEnabled); err != nil {
		return
	}
	if res.ClientKey, err = conf.FieldString(ao2FieldClientKey); err != nil {
		return
	}
	if res.ClientSecret, err = conf.FieldString(ao2FieldClientSecret); err != nil {
		return
	}
	if res.TokenURL, err = conf.FieldString(ao2FieldTokenURL); err != nil {
		return
	}
	if res.Scopes, err = conf.FieldStringList(ao2FieldScopes); err != nil {
		return
	}
	var endpointParams map[string]*service.ParsedConfig
	if endpointParams, err = conf.FieldAnyMap(ao2FieldEndpointParams); err != nil {
		return
	}
	res.EndpointParams = map[string][]string{}
	for k, v := range endpointParams {
		if res.EndpointParams[k], err = v.FieldStringList(); err != nil {
			return
		}
	}
	return
}

//------------------------------------------------------------------------------

const (
	ajFieldEnabled        = "enabled"
	ajFieldPrivateKeyFile = "private_key_file"
	ajFieldSigningMethod  = "signing_method"
	ajFieldClaims         = "claims"
	ajFieldHeaders        = "headers"
)

func jwtFieldSpec() *service.ConfigField {
	return service.NewObjectField(aFieldJWT,
		service.NewBoolField(ajFieldEnabled).
			Description("Whether to use JWT authentication in requests.").
			Default(false),

		service.NewStringField(ajFieldPrivateKeyFile).
			Description("A file with the PEM encoded via PKCS1 or PKCS8 as private key.").
			Default(""),

		service.NewStringField(ajFieldSigningMethod).
			Description("A method used to sign the token such as RS256, RS384, RS512 or EdDSA.").
			Default(""),

		service.NewAnyMapField(ajFieldClaims).
			Description("A value used to identify the claims that issued the JWT.").
			Default(map[string]any{}).
			Advanced(),

		service.NewAnyMapField(ajFieldHeaders).
			Description("Add optional key/value headers to the JWT.").
			Default(map[string]any{}).
			Advanced(),
	).
		Description("BETA: Allows you to specify JWT authentication.").
		Advanced()
}

func jwtAuthFromParsed(conf *service.ParsedConfig) (res JWTConfig, err error) {
	res = NewJWTConfig()
	if !conf.Contains(aFieldJWT) {
		return
	}
	conf = conf.Namespace(aFieldJWT)
	if res.Enabled, err = conf.FieldBool(ajFieldEnabled); err != nil {
		return
	}
	var claimsConfs map[string]*service.ParsedConfig
	if claimsConfs, err = conf.FieldAnyMap(ajFieldClaims); err != nil {
		return
	}
	for k, v := range claimsConfs {
		if res.Claims[k], err = v.FieldAny(); err != nil {
			return
		}
	}
	var headersConfs map[string]*service.ParsedConfig
	if headersConfs, err = conf.FieldAnyMap(ajFieldHeaders); err != nil {
		return
	}
	for k, v := range headersConfs {
		if res.Headers[k], err = v.FieldAny(); err != nil {
			return
		}
	}
	if res.SigningMethod, err = conf.FieldString(ajFieldSigningMethod); err != nil {
		return
	}
	if res.PrivateKeyFile, err = conf.FieldString(ajFieldPrivateKeyFile); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

const (
	av4Field        = "aws_v4"
	av4FieldEnabled = "enabled"
	av4FieldService = "service"
)

func awsV4FieldSpec() *service.ConfigField {
	awsSessionFields := config.SessionFields()
	regionField := awsSessionFields[0]
	credentialsField := awsSessionFields[2]

	return service.NewObjectField("aws_v4",
		service.NewBoolField(av4FieldEnabled).
			Description("Whether to use AWS V4 authentication in requests.").
			Default(false),
		regionField,
		credentialsField,
		service.NewStringField(av4FieldService).
			Description("Optional service name to use for the request").
			Default(""),
	)
}

func awsV4FromParsed(conf *service.ParsedConfig) (res AWSV4Config, err error) {
	res = NewAWSV4Config()
	if !conf.Contains(av4Field) {
		return
	}
	conf = conf.Namespace(av4Field)
	if res.Enabled, err = conf.FieldBool(av4FieldEnabled); err != nil {
		return
	}
	session, err := aws.GetSession(context.Background(), conf)
	if err != nil {
		return
	}
	if res.Service, err = conf.FieldString(av4FieldService); err != nil {
		return
	}
	res.Region = session.Region
	res.Creds, err = session.Credentials.Retrieve(context.Background())
	return
}
