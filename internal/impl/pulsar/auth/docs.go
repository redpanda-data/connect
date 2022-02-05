package auth

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpec returns documentation authentication specs for Pulsar components
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("auth", "Optional configuration of Pulsar authentication methods.").WithChildren(
		docs.FieldAdvanced("oauth2", "Parameters for Pulsar OAuth2 authentication.").WithChildren(
			docs.FieldBool("enabled", "Whether OAuth2 is enabled.", true),
			docs.FieldString("audience", "OAuth2 audience."),
			docs.FieldString("issuer_url", "OAuth2 issuer URL."),
			docs.FieldString("private_key_file", "File containing the private key."),
		),
		docs.FieldAdvanced("token", "Parameters for Pulsar Token authentication.").WithChildren(
			docs.FieldBool("enabled", "Whether Token Auth is enabled.", true),
			docs.FieldString("token", "Actual base64 encoded token."),
		),
	).AtVersion("3.60.0")
}
