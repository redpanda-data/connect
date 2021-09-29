package auth

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpec returns documentation authentication specs for NATS components
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("auth", "Optional configuration of Pulsar authentication methods.").WithChildren(
		docs.FieldAdvanced("oauth2", "Parameters for Pulsar OAuth2 authentication.").WithChildren(
			docs.FieldBool("enabled", "Flag indicating whether OAuth2 should be used or not.", true),
			docs.FieldString("client_id", "OAuth2 Client ID."),
			docs.FieldString("private_key", "OAuth2 private key."),
			docs.FieldString("audience", "OAuth2 audience."),
			docs.FieldString("issuer_url", "OAuth2 issuer URL."),
		),
		docs.FieldAdvanced("token", "Parameters for Pulsar Token authentication.").WithChildren(
			docs.FieldBool("enabled", "Flag indicating whether Token Auth should be used or not.", true),
			docs.FieldString("token", "The actual token."),
		),
	)
}
