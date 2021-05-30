package auth

import "github.com/Jeffail/benthos/v3/internal/docs"

// BasicAuthFieldSpec returns a basic authentication field spec.
func BasicAuthFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("basic_auth",
		"Allows you to specify basic authentication.",
	).WithChildren(
		docs.FieldCommon(
			"enabled", "Whether to use basic authentication in requests.",
		).HasType(docs.FieldBool).HasDefault(false),

		docs.FieldCommon(
			"username", "A username to authenticate as.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"password", "A password to authenticate with.",
		).HasType(docs.FieldString).HasDefault(""),
	)
}

func oAuthFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("oauth",
		"Allows you to specify open authentication via OAuth version 1.",
	).WithChildren(
		docs.FieldCommon(
			"enabled", "Whether to use OAuth version 1 in requests.",
		).HasType(docs.FieldBool).HasDefault(false),

		docs.FieldCommon(
			"consumer_key", "A value used to identify the client to the service provider.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"consumer_secret", "A secret used to establish ownership of the consumer key.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"access_token", "A value used to gain access to the protected resources on behalf of the user.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"access_token_secret", "A secret provided in order to establish ownership of a given access token.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"request_url", "The URL of the OAuth provider.",
		).HasType(docs.FieldString).HasDefault(""),
	)
}

func oAuth2FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("oauth2",
		"Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.",
	).WithChildren(
		docs.FieldCommon(
			"enabled", "Whether to use OAuth version 2 in requests.",
		).HasType(docs.FieldBool).HasDefault(false),

		docs.FieldCommon(
			"client_key", "A value used to identify the client to the token provider.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"client_secret", "A secret used to establish ownership of the client key.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"token_url", "The URL of the token provider.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldAdvanced(
			"scopes", "A list of optional requested permissions.",
		).Array().AtVersion("3.45.0").HasType(docs.FieldString),
	)
}

func jwtFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("jwt",
		"BETA: Allows you to specify JWT authentication.",
	).WithChildren(
		docs.FieldCommon(
			"enabled", "Whether to use JWT authentication in requests.",
		).HasType(docs.FieldBool).HasDefault(false),

		docs.FieldCommon(
			"private_key_file", "A file with the PEM encoded via PKCS1 or PKCS8 as private key.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldCommon(
			"signing_method", "A method used to sign the token such as RS256, RS384 or RS512.",
		).HasType(docs.FieldString).HasDefault(""),

		docs.FieldAdvanced(
			"claims", "A value used to identify the claims that issued the JWT.",
		).Map().HasType(docs.FieldString),
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
