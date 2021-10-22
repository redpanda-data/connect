package session

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpecs returns documentation specs for AWS session fields.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("region", "The AWS region to target."),
		docs.FieldAdvanced("endpoint", "Allows you to specify a custom endpoint for the AWS API."),
		docs.FieldAdvanced("credentials", "Optional manual configuration of AWS credentials to use. More information can be found [in this document](/docs/guides/cloud/aws).").WithChildren(
			docs.FieldAdvanced("profile", "A profile from `~/.aws/credentials` to use."),
			docs.FieldAdvanced("id", "The ID of credentials to use."),
			docs.FieldAdvanced("secret", "The secret for the credentials being used."),
			docs.FieldAdvanced("token", "The token for the credentials being used, required when using short term credentials."),
			docs.FieldAdvanced("role", "A role ARN to assume."),
			docs.FieldAdvanced("role_external_id", "An external ID to provide when assuming a role."),
		),
	}
}
