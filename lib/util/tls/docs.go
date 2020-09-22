package tls

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpec returns a spec for a common TLS field.
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("tls", "Custom TLS settings can be used to override system defaults.").WithChildren(
		docs.FieldCommon("enabled", "Whether custom TLS settings are enabled."),
		docs.FieldCommon("skip_cert_verify", "Whether to skip server side certificate verification."),
		docs.FieldCommon("root_cas_file", "The path of a root certificate authority file to use."),
		docs.FieldCommon("client_certs", "A list of client certificates to use. For each certificate either the fields `cert` and `key`, or `cert_file` and `key_file` should be specified, but not both.",
			[]interface{}{
				map[string]interface{}{
					"cert": "foo",
					"key":  "bar",
				},
			},
			[]interface{}{
				map[string]interface{}{
					"cert_file": "./example.pem",
					"key_file":  "./example.key",
				},
			},
		).HasType(docs.FieldArray).WithChildren(
			docs.FieldCommon("cert", "A plain text certificate to use.").HasDefault(""),
			docs.FieldCommon("key", "A plain text certificate key to use.").HasDefault(""),
			docs.FieldCommon("cert_file", "The path to a certificate to use.").HasDefault(""),
			docs.FieldCommon("key_file", "The path of a certificate key to use.").HasDefault(""),
		),
	)
}
