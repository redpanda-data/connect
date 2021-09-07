package auth

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpec returns documentation authentication specs for NATS components
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("auth", "Optional configuration of NATS authentication parameters. More information can be found [in this document](/docs/guides/nats).").WithChildren(
		docs.FieldString("nkey_file", "An optional file containing a NKey seed.", "./seed.nk").Optional(),
		docs.FieldString("user_credentials_file", "An optional file containing user credentials which consist of an user JWT and corresponding NKey seed.", "./user.creds").Optional(),
	)
}
