package config

import "github.com/benthosdev/benthos/v4/public/service"

// SessionFields defines a re-usable set of config fields for an AWS session
// that is compatible with the public service APIs and avoids importing the full
// AWS dependencies.
func SessionFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("region").
			Description("The AWS region to target.").
			Default("").
			Advanced(),
		service.NewStringField("endpoint").
			Description("Allows you to specify a custom endpoint for the AWS API.").
			Default("").
			Advanced(),
		service.NewObjectField("credentials",
			service.NewStringField("profile").
				Description("A profile from `~/.aws/credentials` to use.").
				Default(""),
			service.NewStringField("id").
				Description("The ID of credentials to use.").
				Default("").Advanced(),
			service.NewStringField("secret").
				Description("The secret for the credentials being used.").
				Default("").Advanced().Secret(),
			service.NewStringField("token").
				Description("The token for the credentials being used, required when using short term credentials.").
				Default("").Advanced(),
			service.NewBoolField("from_ec2_role").
				Description("Use the credentials of a host EC2 machine configured to assume [an IAM role associated with the instance](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html).").
				Default(false).Version("4.2.0"),
			service.NewStringField("role").
				Description("A role ARN to assume.").
				Default("").Advanced(),
			service.NewStringField("role_external_id").
				Description("An external ID to provide when assuming a role.").
				Default("").Advanced()).
			Advanced().
			Description("Optional manual configuration of AWS credentials to use. More information can be found [in this document](/docs/guides/cloud/aws)."),
	}
}
