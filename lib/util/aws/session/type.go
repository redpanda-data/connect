// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	ID     string `json:"id" yaml:"id"`
	Secret string `json:"secret" yaml:"secret"`
	Token  string `json:"token" yaml:"token"`
	Role   string `json:"role" yaml:"role"`
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
			ID:     "",
			Secret: "",
			Token:  "",
			Role:   "",
		},
		Endpoint: "",
		Region:   "eu-west-1",
	}
}

//------------------------------------------------------------------------------

// GetSession attempts to create an AWS session based on Config.
func (c Config) GetSession() (*session.Session, error) {
	awsConf := aws.NewConfig()
	if len(c.Region) > 0 {
		awsConf = awsConf.WithRegion(c.Region)
	}

	if len(c.Endpoint) > 0 {
		awsConf = awsConf.WithEndpoint(c.Endpoint)
	}

	if len(c.Credentials.ID) > 0 {
		awsConf = awsConf.WithCredentials(credentials.NewStaticCredentials(
			c.Credentials.ID,
			c.Credentials.Secret,
			c.Credentials.Token,
		))
	}

	sess, err := session.NewSession(awsConf)
	if err != nil {
		return nil, err
	}

	if len(c.Credentials.Role) > 0 {
		sess.Config = sess.Config.WithCredentials(
			stscreds.NewCredentials(sess, c.Credentials.Role),
		)
	}

	return sess, nil
}

//------------------------------------------------------------------------------
