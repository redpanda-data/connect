// Copyright 2026 Redpanda Data, Inc.
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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/mongodb"
)

const (
	mongoDBAWSMechanism = "MONGODB-AWS"

	// minSTSSessionDuration is the minimum session duration accepted by STS
	// AssumeRole.
	minSTSSessionDuration = 15 * time.Minute
)

func init() {
	mongodb.AWSOptFn = awsIAMCredentials
}

type roleConfig struct {
	arn        string
	externalID string
}

// awsIAMCredentials parses and validates the `aws` config block eagerly, so
// that misconfiguration is reported at startup, and returns a builder which
// resolves the MONGODB-AWS credential on each connection attempt.
func awsIAMCredentials(awsConf *service.ParsedConfig, log *service.Logger) (mongodb.CredentialBuilder, error) {
	if enabled, _ := awsConf.FieldBool(mongodb.FieldAWSIAMAuthEnabled); !enabled {
		return nil, nil
	}

	id, _ := awsConf.FieldString(mongodb.FieldAWSIAMAuthID)
	secret, _ := awsConf.FieldString(mongodb.FieldAWSIAMAuthSecret)
	token, _ := awsConf.FieldString(mongodb.FieldAWSIAMAuthToken)

	if (id == "") != (secret == "") {
		return nil, errors.New("aws.id and aws.secret must both be set when either is provided")
	}
	if token != "" && id == "" {
		return nil, errors.New("aws.token requires aws.id and aws.secret to also be set")
	}

	region, _ := awsConf.FieldString(mongodb.FieldAWSIAMAuthRegion)

	roleConfigs, err := parseRoleConfigs(awsConf)
	if err != nil {
		return nil, err
	}

	if len(roleConfigs) == 0 {
		// With no roles to assume, hand the driver whatever static keys were
		// configured. When they are empty the driver resolves the ambient AWS
		// credential chain itself (env vars, ECS, EC2 instance profile, EKS
		// web identity) and refreshes expiring credentials automatically.
		cred := credentialFromKeys(id, secret, token)
		return func(context.Context) (*options.Credential, error) {
			return cred, nil
		}, nil
	}

	sessionDuration, err := awsConf.FieldDuration(mongodb.FieldAWSIAMAuthSessionDuration)
	if err != nil {
		return nil, err
	}
	// STS rejects anything below its 15 minute minimum, and role chaining is
	// additionally capped at an hour, which only AWS can enforce.
	if sessionDuration < minSTSSessionDuration {
		return nil, fmt.Errorf("aws.session_duration must be at least %v", minSTSSessionDuration)
	}

	return func(ctx context.Context) (*options.Credential, error) {
		var opts []func(*awsconfig.LoadOptions) error
		if region != "" {
			opts = append(opts, awsconfig.WithRegion(region))
		}
		if id != "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(id, secret, token)))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("unable to load AWS config: %w", err)
		}
		awsCfg = assumeRoleChain(awsCfg, roleConfigs, sessionDuration, log)

		// The mongo driver has no credential-provider hook, so assumed-role
		// session credentials are snapshotted here and re-resolved on each
		// connection attempt, i.e. whenever the component rebuilds its client.
		creds, err := awsCfg.Credentials.Retrieve(ctx)
		if err != nil {
			return nil, fmt.Errorf("retrieving assumed-role credentials: %w", err)
		}
		if creds.CanExpire {
			log.Debugf("Assumed-role credentials resolved, expire at %s; they are refreshed when the component reconnects", creds.Expires)
		}
		return credentialFromKeys(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken), nil
	}, nil
}

func credentialFromKeys(id, secret, token string) *options.Credential {
	cred := &options.Credential{
		AuthMechanism: mongoDBAWSMechanism,
		Username:      id,
		Password:      secret,
	}
	if token != "" {
		cred.AuthMechanismProperties = map[string]string{"AWS_SESSION_TOKEN": token}
	}
	return cred
}

func parseRoleConfigs(awsConf *service.ParsedConfig) ([]roleConfig, error) {
	var rolesConfs []*service.ParsedConfig
	if awsConf.Contains(mongodb.FieldAWSIAMAuthRoles) {
		var err error
		if rolesConfs, err = awsConf.FieldObjectList(mongodb.FieldAWSIAMAuthRoles); err != nil {
			return nil, err
		}
	}

	var roles []roleConfig
	singleRole, _ := awsConf.FieldString(mongodb.FieldAWSIAMAuthRole)
	if singleRole != "" {
		if len(rolesConfs) > 0 {
			return nil, errors.New("aws.role and aws.roles cannot both be set; use roles for chaining")
		}
		externalID, _ := awsConf.FieldString(mongodb.FieldAWSIAMAuthRoleExternalID)
		roles = append(roles, roleConfig{arn: singleRole, externalID: externalID})
	}

	for i, conf := range rolesConfs {
		arn, _ := conf.FieldString(mongodb.FieldAWSIAMAuthRole)
		if arn == "" {
			return nil, fmt.Errorf("roles[%d].role is required for IAM authentication", i)
		}
		externalID, _ := conf.FieldString(mongodb.FieldAWSIAMAuthRoleExternalID)
		roles = append(roles, roleConfig{arn: arn, externalID: externalID})
	}
	return roles, nil
}

// assumeRoleChain iterates through one or more roles enabling the user to
// chain them (ie, from local role, privileged then cross-account). The
// resulting credentials provider is lazy: the STS calls happen when the
// credentials are first retrieved.
func assumeRoleChain(awsCfg aws.Config, roles []roleConfig, sessionDuration time.Duration, log *service.Logger) aws.Config {
	currentConfig := awsCfg
	for _, role := range roles {
		log.Debugf("Assuming role '%s'", role.arn)
		stsClient := sts.NewFromConfig(currentConfig)
		provider := stscreds.NewAssumeRoleProvider(stsClient, role.arn, func(opts *stscreds.AssumeRoleOptions) {
			opts.Duration = sessionDuration
			if role.externalID != "" {
				opts.ExternalID = &role.externalID
				log.Debugf("Using external ID for role '%s'", role.arn)
			}
		})
		currentConfig.Credentials = aws.NewCredentialsCache(provider)
	}

	return currentConfig
}
