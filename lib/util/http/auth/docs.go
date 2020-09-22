package auth

import "github.com/Jeffail/benthos/v3/internal/docs"

// BasicAuthFieldSpec returns a basic authentication field spec.
func BasicAuthFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("basic_auth",
		"Allows you to specify basic authentication.",
	).WithChildren(
		docs.FieldCommon("enabled", "Whether to use basic authentication in requests."),
		docs.FieldCommon("username", "A username to authenticate as."),
		docs.FieldCommon("password", "A password to authenticate with."),
	)
}

func oAuthFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("oauth",
		"Allows you to specify open authentication via OAuth version 1.",
	).WithChildren(
		docs.FieldCommon("enabled", "Whether to use OAuth version 1 in requests."),
		docs.FieldCommon("consumer_key", "A value used to identify the client to the service provider."),
		docs.FieldCommon("consumer_secret", "A secret used to establish ownership of the consumer key."),
		docs.FieldCommon("access_token", "A value used to gain access to the protected resources on behalf of the user."),
		docs.FieldCommon("access_token_secret", "A secret provided in order to establish ownership of a given access token."),
		docs.FieldCommon("request_url", "The URL of the OAuth provider."),
	)
}

func oAuth2FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("oauth2",
		"Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.",
	).WithChildren(
		docs.FieldCommon("enabled", "Whether to use OAuth version 2 in requests."),
		docs.FieldCommon("client_key", "A value used to identify the client to the token provider."),
		docs.FieldCommon("client_secret", "A secret used to establish ownership of the client key."),
		docs.FieldCommon("token_url", "The URL of the token provider."),
	)
}

// FieldSpecs returns a map of field specs for an auth type.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		oAuthFieldSpec(),
		BasicAuthFieldSpec(),
	}
}

// FieldSpecsExpanded includes OAuth2 fields that might not be appropriate for
// all components.
func FieldSpecsExpanded() docs.FieldSpecs {
	return docs.FieldSpecs{
		oAuthFieldSpec(),
		oAuth2FieldSpec(),
		BasicAuthFieldSpec(),
	}
}
