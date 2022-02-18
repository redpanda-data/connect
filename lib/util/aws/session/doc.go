package session

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpecs returns documentation specs for AWS session fields.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("region", "The AWS region to target.").Advanced(),
		docs.FieldString("endpoint", "Allows you to specify a custom endpoint for the AWS API.").Advanced(),
		docs.FieldObject("credentials", "Optional manual configuration of AWS credentials to use. More information can be found [in this document](/docs/guides/cloud/aws).").
			Advanced().
			WithChildren(
				docs.FieldString("profile", "A profile from `~/.aws/credentials` to use."),
				docs.FieldString("id", "The ID of credentials to use."),
				docs.FieldString("secret", "The secret for the credentials being used."),
				docs.FieldString("token", "The token for the credentials being used, required when using short term credentials."),
				docs.FieldString("role", "A role ARN to assume."),
				docs.FieldString("role_external_id", "An external ID to provide when assuming a role."),
			),
	}
}
