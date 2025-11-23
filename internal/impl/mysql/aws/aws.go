// Copyright 2025 Redpanda Data, Inc.
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
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/go-sql-driver/mysql"

	"github.com/redpanda-data/benthos/v4/public/service"

	mysqlimpl "github.com/redpanda-data/connect/v4/internal/impl/mysql"
)

type roleConfig struct {
	arn        string
	externalID string
}

func init() {
	mysqlimpl.AWSOptFn = awsIAMAuth
}

func awsIAMAuth(ctx context.Context, awsConf *service.ParsedConfig, dbConf *mysql.Config, log *service.Logger) (mysqlimpl.TokenBuilder, error) {
	if enabled, _ := awsConf.FieldBool(mysqlimpl.FieldAWSIAMAuthEnabled); !enabled {
		return nil, nil
	}

	var (
		err    error
		awsCfg aws.Config

		endpoint    string
		region      string
		roleConfigs []roleConfig
	)
	if awsCfg, err = awsconfig.LoadDefaultConfig(ctx); err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}
	if endpoint, err = awsConf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if region, err = awsConf.FieldString("region"); err != nil {
		return nil, err
	} else if region != "" {
		awsCfg.Region = region
	}
	if awsCfg.Region == "" {
		return nil, errors.New("aws.region is required for IAM authentication")
	}

	// parse aws.role and aws.roles[]
	role, _ := parseRoleConfig(awsConf)
	roleConfigs = append(roleConfigs, role...)

	if rolesConfs, err := awsConf.FieldObjectList("roles"); err != nil {
		return nil, err
	} else {
		for _, conf := range rolesConfs {
			if roles, err := parseRoleConfig(conf); err != nil {
				return nil, err
			} else {
				for i, v := range roles {
					if v.arn == "" {
						return nil, fmt.Errorf("roles[%d].role is required for IAM authentication", i)
					}
				}
				roleConfigs = append(roleConfigs, roles...)
			}
		}
	}

	// tokenBuilder will be called upon component connection to refresh token/password and reconnect.
	// Tokens last ~15 minutes and will only need refreshing after a connection is lost.
	tokenBuilder := func(ctx context.Context) error {
		if len(roleConfigs) > 0 {
			if awsCfg, err = assumeRoleChain(ctx, awsCfg, roleConfigs, log); err != nil {
				return fmt.Errorf("assuming role based on configured roles: %w", err)
			}
		}
		password, err := auth.BuildAuthToken(ctx, endpoint, awsCfg.Region, dbConf.User, awsCfg.Credentials)
		if err != nil {
			return fmt.Errorf("building IAM auth token: %w", err)
		}
		dbConf.Passwd = password

		log.Debug("IAM authentication token generated successfully")
		return nil
	}
	return tokenBuilder, nil
}

// assumeRoleChain iterates through one or more roles enabling the user to chain elevation them (ie, from local role, privileged then cross-account).
// If no roles are set, AWS SDK will check for environment configured roles and automatically assume them.
func assumeRoleChain(ctx context.Context, awsCfg aws.Config, roles []roleConfig, log *service.Logger) (aws.Config, error) {
	currentConfig := awsCfg
	for _, role := range roles {
		if role.arn == "" {
			continue
		}

		// Create credentials provider for this role
		stsClient := sts.NewFromConfig(currentConfig)
		provider := stscreds.NewAssumeRoleProvider(stsClient, role.arn, func(opts *stscreds.AssumeRoleOptions) {
			if role.externalID != "" {
				opts.ExternalID = &role.externalID
				log.Debugf("Using external ID for role '%s'", role.arn)
			}
		})
		currentConfig.Credentials = aws.NewCredentialsCache(provider)

		// Verify the role assumption worked
		identity, err := sts.NewFromConfig(currentConfig).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err != nil {
			return aws.Config{}, fmt.Errorf("verifying role assumption for '%s': %w", role.arn, err)
		}

		log.Debugf("Successfully assumed role '%s' with identity '%s'", role.arn, *identity.Arn)
	}

	return currentConfig, nil
}

func parseRoleConfig(awsConf *service.ParsedConfig) ([]roleConfig, error) {
	var roles []roleConfig
	if role, err := awsConf.FieldString("role"); err != nil {
		return nil, err
	} else if externalID, err := awsConf.FieldString("role_external_id"); err != nil {
		return nil, err
	} else {
		roles = append(roles, roleConfig{role, externalID})
	}

	return roles, nil
}
