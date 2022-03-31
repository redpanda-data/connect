package auth

import "github.com/benthosdev/benthos/v4/internal/docs"

// BasicAuthFieldSpec returns a basic authentication field spec.
func BasicAuthFieldSpec() docs.FieldSpec {
	return docs.FieldObject("basic_auth",
		"Allows you to specify basic authentication.",
	).WithChildren(
		docs.FieldBool("enabled", "Whether to use basic authentication in requests.").HasDefault(false),
		docs.FieldString("username", "A username to authenticate as.").HasDefault(""),
		docs.FieldString("password", "A password to authenticate with.").HasDefault(""),
	).Advanced()
}

func oAuthFieldSpec() docs.FieldSpec {
	return docs.FieldObject("oauth",
		"Allows you to specify open authentication via OAuth version 1.",
	).Advanced().WithChildren(
		docs.FieldBool(
			"enabled", "Whether to use OAuth version 1 in requests.",
		).HasDefault(false),

		docs.FieldString(
			"consumer_key", "A value used to identify the client to the service provider.",
		).HasDefault(""),

		docs.FieldString(
			"consumer_secret", "A secret used to establish ownership of the consumer key.",
		).HasDefault(""),

		docs.FieldString(
			"access_token", "A value used to gain access to the protected resources on behalf of the user.",
		).HasDefault(""),

		docs.FieldString(
			"access_token_secret", "A secret provided in order to establish ownership of a given access token.",
		).HasDefault(""),
	)
}

func oAuth2FieldSpec() docs.FieldSpec {
	return docs.FieldObject("oauth2",
		"Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.",
	).Advanced().WithChildren(
		docs.FieldBool(
			"enabled", "Whether to use OAuth version 2 in requests.",
		).HasDefault(false),

		docs.FieldString(
			"client_key", "A value used to identify the client to the token provider.",
		).HasDefault(""),

		docs.FieldString(
			"client_secret", "A secret used to establish ownership of the client key.",
		).HasDefault(""),

		docs.FieldString(
			"token_url", "The URL of the token provider.",
		).HasDefault(""),

		docs.FieldString(
			"scopes", "A list of optional requested permissions.",
		).Array().Advanced().AtVersion("3.45.0"),
	)
}

func jwtFieldSpec() docs.FieldSpec {
	return docs.FieldObject("jwt",
		"BETA: Allows you to specify JWT authentication.",
	).Advanced().WithChildren(
		docs.FieldBool(
			"enabled", "Whether to use JWT authentication in requests.",
		).HasDefault(false),

		docs.FieldString(
			"private_key_file", "A file with the PEM encoded via PKCS1 or PKCS8 as private key.",
		).HasDefault(""),

		docs.FieldString(
			"signing_method", "A method used to sign the token such as RS256, RS384 or RS512.",
		).HasDefault(""),

		docs.FieldAnything(
			"claims", "A value used to identify the claims that issued the JWT.",
		).Map().Advanced(),
	)
}

// FieldSpecs returns a map of field specs for an auth type.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		oAuthFieldSpec(),
		BasicAuthFieldSpec(),
		jwtFieldSpec(),
	}
}

// FieldSpecsExpanded includes OAuth2 and JWT fields that might not be appropriate for all components.
func FieldSpecsExpanded() docs.FieldSpecs {
	return docs.FieldSpecs{
		oAuthFieldSpec(),
		oAuth2FieldSpec(),
		jwtFieldSpec(),
		BasicAuthFieldSpec(),
	}
}
