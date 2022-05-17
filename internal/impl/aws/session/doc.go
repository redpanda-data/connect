package session

import "github.com/benthosdev/benthos/v4/internal/docs"

// FieldSpecs returns documentation specs for AWS session fields.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("region", "The AWS region to target.").Advanced().HasDefault(""),
		docs.FieldString("endpoint", "Allows you to specify a custom endpoint for the AWS API.").Advanced().HasDefault(""),
		docs.FieldObject("credentials", "Optional manual configuration of AWS credentials to use. More information can be found [in this document](/docs/guides/cloud/aws).").
			Advanced().
			WithChildren(
				docs.FieldString("profile", "A profile from `~/.aws/credentials` to use.").HasDefault(""),
				docs.FieldString("id", "The ID of credentials to use.").HasDefault(""),
				docs.FieldString("secret", "The secret for the credentials being used.").HasDefault(""),
				docs.FieldString("token", "The token for the credentials being used, required when using short term credentials.").HasDefault(""),
				docs.FieldBool("from_ec2_role", "Use the credentials of a host EC2 machine configured to assume [an IAM role associated with the instance](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html).").HasDefault(false).AtVersion("4.2.0"),
				docs.FieldString("role", "A role ARN to assume.").HasDefault(""),
				docs.FieldString("role_external_id", "An external ID to provide when assuming a role.").HasDefault(""),
			),
	}
}
