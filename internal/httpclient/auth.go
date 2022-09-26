package httpclient

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/docs/interop"
	"github.com/benthosdev/benthos/v4/public/service"
)

// OldBasicAuthFieldSpec returns a basic authentication field spec.
func OldBasicAuthFieldSpec() docs.FieldSpec {
	return interop.Unwrap(basicAuthField())
}

// OldAuthFieldSpecs returns a map of field specs for an auth type.
func OldAuthFieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		interop.Unwrap(oAuthFieldSpec()),
		interop.Unwrap(basicAuthField()),
		interop.Unwrap(jwtFieldSpec()),
	}
}

// OldAuthFieldSpecsExpanded includes OAuth2 and JWT fields that might not be
// appropriate for all components.
func OldAuthFieldSpecsExpanded() docs.FieldSpecs {
	return docs.FieldSpecs{
		interop.Unwrap(oAuthFieldSpec()),
		interop.Unwrap(oAuth2FieldSpec()),
		interop.Unwrap(jwtFieldSpec()),
		interop.Unwrap(basicAuthField()),
	}
}

func basicAuthField() *service.ConfigField {
	return service.NewObjectField("basic_auth",
		service.NewBoolField("enabled").
			Description("Whether to use basic authentication in requests.").
			Default(false),

		service.NewStringField("username").
			Description("A username to authenticate as.").
			Default(""),

		service.NewStringField("password").
			Description("A password to authenticate with.").
			Default(""),
	).Description("Allows you to specify basic authentication.").
		Advanced()
}

func oAuthFieldSpec() *service.ConfigField {
	return service.NewObjectField("oauth",
		service.NewBoolField("enabled").
			Description("Whether to use OAuth version 1 in requests.").
			Default(false),

		service.NewStringField("consumer_key").
			Description("A value used to identify the client to the service provider.").
			Default(""),

		service.NewStringField("consumer_secret").
			Description("A secret used to establish ownership of the consumer key.").
			Default(""),

		service.NewStringField("access_token").
			Description("A value used to gain access to the protected resources on behalf of the user.").
			Default(""),

		service.NewStringField("access_token_secret").
			Description("A secret provided in order to establish ownership of a given access token.").
			Default(""),
	).
		Description("Allows you to specify open authentication via OAuth version 1.").
		Advanced()
}

func oAuth2FieldSpec() *service.ConfigField {
	return service.NewObjectField("oauth2",
		service.NewBoolField("enabled").
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),

		service.NewStringField("client_key").
			Description("A value used to identify the client to the token provider.").
			Default(""),

		service.NewStringField("client_secret").
			Description("A secret used to establish ownership of the client key.").
			Default(""),

		service.NewStringField("token_url").
			Description("The URL of the token provider.").
			Default(""),

		service.NewStringListField("scopes").
			Description("A list of optional requested permissions.").
			Advanced().
			Version("3.45.0"),
	).
		Description("Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.").
		Advanced()
}

func jwtFieldSpec() *service.ConfigField {
	return service.NewObjectField("jwt",
		service.NewBoolField("enabled").
			Description("Whether to use JWT authentication in requests.").
			Default(false),

		service.NewStringField("private_key_file").
			Description("A file with the PEM encoded via PKCS1 or PKCS8 as private key.").
			Default(""),

		service.NewStringField("signing_method").
			Description("A method used to sign the token such as RS256, RS384 or RS512.").
			Default(""),

		service.NewAnyMapField("claims").
			Description("A value used to identify the claims that issued the JWT.").
			Advanced(),

		service.NewAnyMapField("headers").
			Description("Add optional key/value headers to the JWT.").
			Advanced(),
	).
		Description("BETA: Allows you to specify JWT authentication.").
		Advanced()
}
