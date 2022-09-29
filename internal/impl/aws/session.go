package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"

	bsession "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/public/service"
)

// GetSession attempts to create an AWS session based on the parsedConfig.
func GetSession(parsedConf *service.ParsedConfig, opts ...func(*aws.Config)) (*session.Session, error) {
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

	if useEC2, _ := parsedConf.FieldBool("from_ec2_role"); useEC2 {
		sess.Config = sess.Config.WithCredentials(ec2rolecreds.NewCredentials(sess))
	}

	return sess, nil
}

// GetSessionFromConf attempts to create an AWS session based on Config.
func GetSessionFromConf(c bsession.Config, opts ...func(*aws.Config)) (*session.Session, error) {
	awsConf := aws.NewConfig()
	if len(c.Region) > 0 {
		awsConf = awsConf.WithRegion(c.Region)
	}

	if len(c.Endpoint) > 0 {
		awsConf = awsConf.WithEndpoint(c.Endpoint)
	}

	if len(c.Credentials.Profile) > 0 {
		awsConf = awsConf.WithCredentials(credentials.NewSharedCredentials(
			"", c.Credentials.Profile,
		))
	} else if len(c.Credentials.ID) > 0 {
		awsConf = awsConf.WithCredentials(credentials.NewStaticCredentials(
			c.Credentials.ID,
			c.Credentials.Secret,
			c.Credentials.Token,
		))
	}

	for _, opt := range opts {
		opt(awsConf)
	}

	sess, err := session.NewSession(awsConf)
	if err != nil {
		return nil, err
	}

	if len(c.Credentials.Role) > 0 {
		var opts []func(*stscreds.AssumeRoleProvider)
		if len(c.Credentials.ExternalID) > 0 {
			opts = []func(*stscreds.AssumeRoleProvider){
				func(p *stscreds.AssumeRoleProvider) {
					p.ExternalID = &c.Credentials.ExternalID
				},
			}
		}
		sess.Config = sess.Config.WithCredentials(
			stscreds.NewCredentials(sess, c.Credentials.Role, opts...),
		)
	}

	if c.Credentials.UseEC2Creds {
		sess.Config = sess.Config.WithCredentials(ec2rolecreds.NewCredentials(sess))
	}

	return sess, nil
}
