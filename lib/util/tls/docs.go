package tls

import "github.com/Jeffail/benthos/v3/lib/x/docs"

// FieldSpec returns a spec for a common TLS field.
func FieldSpec() docs.FieldSpec {
	return docs.FieldSpec{
		Name: "tls",
		Description: `Custom TLS settings can be used to override system defaults. This includes
providing a collection of root certificate authorities, providing a list of
client certificates to use for client verification and skipping certificate
verification.

Client certificates can either be added by file or by raw contents.`,
		Advanced: true,
		Examples: []interface{}{
			map[string]interface{}{
				"enabled": true,
				"client_certs": []interface{}{
					map[string]interface{}{
						"cert_file": "./example.pem",
						"key_file":  "./example.key",
					},
				},
			},
			map[string]interface{}{
				"enabled":          true,
				"skip_cert_verify": true,
				"client_certs": []interface{}{
					map[string]interface{}{
						"cert": "foo",
						"key":  "bar",
					},
				},
			},
		},
	}
}
