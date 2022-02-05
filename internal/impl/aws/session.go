package aws

import (
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

func sessionFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("region").
			Description("The AWS region to target.").
			Default(""),
		service.NewStringField("endpoint").
			Description("Allows you to specify a custom endpoint for the AWS API.").
			Default("").Advanced(),
		service.NewObjectField("credentials",
			service.NewStringField("profile").
				Description("A profile from `~/.aws/credentials` to use.").
				Default(""),
			service.NewStringField("id").
				Description("The ID of credentials to use.").
				Default("").Advanced(),
			service.NewStringField("secret").
				Description("The secret for the credentials being used.").
				Default("").Advanced(),
			service.NewStringField("token").
				Description("The token for the credentials being used, required when using short term credentials.").
				Default("").Advanced(),
			service.NewStringField("role").
				Description("A role ARN to assume.").
				Default("").Advanced(),
			service.NewStringField("role_external_id").
				Description("An external ID to provide when assuming a role.").
				Default("").Advanced()).
			Description("Optional manual configuration of AWS credentials to use. More information can be found [in this document](/docs/guides/cloud/aws)."),
	}
}

func getSession(parsedConf *service.ParsedConfig, opts ...func(*aws.Config)) (*session.Session, error) {
	awsConf := aws.NewConfig()

	if region, _ := parsedConf.FieldString("region"); region != "" {
		awsConf = awsConf.WithRegion(region)
	}
	if endpoint, _ := parsedConf.FieldString("endpoint"); endpoint != "" {
		awsConf = awsConf.WithEndpoint(endpoint)
	}
	if profile, _ := parsedConf.FieldString("credentials", "profile"); profile != "" {
		awsConf = awsConf.WithCredentials(credentials.NewSharedCredentials(
			"", profile,
		))
	} else if id, _ := parsedConf.FieldString("credentials", "id"); id != "" {
		secret, _ := parsedConf.FieldString("credentials", "secret")
		token, _ := parsedConf.FieldString("credentials", "token")
		awsConf = awsConf.WithCredentials(credentials.NewStaticCredentials(
			id, secret, token,
		))
	}

	for _, opt := range opts {
		opt(awsConf)
	}

	sess, err := session.NewSession(awsConf)
	if err != nil {
		return nil, err
	}

	if role, _ := parsedConf.FieldString("credentials", "role"); role != "" {
		var opts []func(*stscreds.AssumeRoleProvider)
		if externalID, _ := parsedConf.FieldString("credentials", "role_external_id"); externalID != "" {
			opts = []func(*stscreds.AssumeRoleProvider){
				func(p *stscreds.AssumeRoleProvider) {
					p.ExternalID = &externalID
				},
			}
		}
		sess.Config = sess.Config.WithCredentials(
			stscreds.NewCredentials(sess, role, opts...),
		)
	}

	return sess, nil
}
