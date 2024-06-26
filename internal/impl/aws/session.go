// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func int64Field(conf *service.ParsedConfig, path ...string) (int64, error) {
	i, err := conf.FieldInt(path...)
	if err != nil {
		return 0, err
	}
	return int64(i), nil
}

// GetSession constructs an AWS session from a parsed config and provided options.
func GetSession(ctx context.Context, parsedConf *service.ParsedConfig, opts ...func(*config.LoadOptions) error) (aws.Config, error) {
	if region, _ := parsedConf.FieldString("region"); region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	credsConf := parsedConf.Namespace("credentials")
	if profile, _ := credsConf.FieldString("profile"); profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	} else if id, _ := credsConf.FieldString("id"); id != "" {
		secret, _ := credsConf.FieldString("secret")
		token, _ := credsConf.FieldString("token")
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			id, secret, token,
		)))
	}

	conf, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return conf, err
	}

	if endpoint, _ := parsedConf.FieldString("endpoint"); endpoint != "" {
		conf.BaseEndpoint = &endpoint
	}

	if role, _ := credsConf.FieldString("role"); role != "" {
		stsSvc := sts.NewFromConfig(conf)

		var stsOpts []func(*stscreds.AssumeRoleOptions)
		if externalID, _ := credsConf.FieldString("role_external_id"); externalID != "" {
			stsOpts = append(stsOpts, func(aro *stscreds.AssumeRoleOptions) {
				aro.ExternalID = &externalID
			})
		}

		creds := stscreds.NewAssumeRoleProvider(stsSvc, role, stsOpts...)
		conf.Credentials = aws.NewCredentialsCache(creds)
	}

	if useEC2, _ := credsConf.FieldBool("from_ec2_role"); useEC2 {
		conf.Credentials = aws.NewCredentialsCache(ec2rolecreds.New())
	}
	return conf, nil
}
