package tls

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpec returns a spec for a common TLS field.
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced(
		"tls", "Custom TLS settings can be used to override system defaults.",
	).WithChildren(
		docs.FieldCommon(
			"enabled", "Whether custom TLS settings are enabled.",
		).HasType(docs.FieldTypeBool).HasDefault(false),

		docs.FieldCommon(
			"skip_cert_verify", "Whether to skip server side certificate verification.",
		).HasType(docs.FieldTypeBool).HasDefault(false),

		docs.FieldAdvanced(
			"enable_renegotiation", "Whether to allow the remote server to repeatedly request renegotiation. Enable this option if you're seeing the error message `local error: tls: no renegotiation`.",
		).AtVersion("3.45.0").HasType(docs.FieldTypeBool).HasDefault(false),

		docs.FieldString(
			"root_cas", "An optional root certificate authority to use. This is a string, representing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate.", "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
		).HasDefault(""),

		docs.FieldString(
			"root_cas_file", "An optional path of a root certificate authority file to use. This is a file, often with a .pem extension, containing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate.", "./root_cas.pem",
		).HasDefault(""),

		docs.FieldCommon(
			"client_certs", "A list of client certificates to use. For each certificate either the fields `cert` and `key`, or `cert_file` and `key_file` should be specified, but not both.",
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
		).Array().WithChildren(
			docs.FieldString("cert", "A plain text certificate to use.").HasDefault(""),
			docs.FieldString("key", "A plain text certificate key to use.").HasDefault(""),
			docs.FieldString("cert_file", "The path to a certificate to use.").HasDefault(""),
			docs.FieldString("key_file", "The path of a certificate key to use.").HasDefault(""),
		),
	)
}
