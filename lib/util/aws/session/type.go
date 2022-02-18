package session

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

//------------------------------------------------------------------------------

// CredentialsConfig contains configuration params for AWS credentials.
type CredentialsConfig struct {
	Profile    string `json:"profile" yaml:"profile"`
	ID         string `json:"id" yaml:"id"`
	Secret     string `json:"secret" yaml:"secret"`
	Token      string `json:"token" yaml:"token"`
	Role       string `json:"role" yaml:"role"`
	ExternalID string `json:"role_external_id" yaml:"role_external_id"`
}

// Config contains configuration fields for an AWS session. This config is
// common across any AWS components.
type Config struct {
	Credentials CredentialsConfig `json:"credentials" yaml:"credentials"`
	Endpoint    string            `json:"endpoint" yaml:"endpoint"`
	Region      string            `json:"region" yaml:"region"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Credentials: CredentialsConfig{
			Profile:    "",
			ID:         "",
			Secret:     "",
			Token:      "",
			Role:       "",
			ExternalID: "",
		},
		Endpoint: "",
		Region:   "",
	}
}

//------------------------------------------------------------------------------

// GetSession attempts to create an AWS session based on Config.
func (c Config) GetSession(opts ...func(*aws.Config)) (*session.Session, error) {
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

	return sess, nil
}

//------------------------------------------------------------------------------
