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
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/benthos/v4/public/service"

	pgstream "github.com/redpanda-data/connect/v4/internal/impl/postgresql"
)

func init() {
	pgstream.AWSOptFn = func(ctx context.Context, awsConf *service.ParsedConfig, dbConf *pgconn.Config, log *service.Logger) (pgstream.TokenBuilder, error) {
		if enabled, _ := awsConf.FieldBool(pgstream.FieldAWSIAMAuthEnabled); !enabled {
			return nil, nil
		}

		var (
			err      error
			awsCfg   aws.Config
			endpoint string
			region   string
		)
		if awsCfg, err = awsconfig.LoadDefaultConfig(ctx); err != nil {
			return nil, fmt.Errorf("unable to load AWS config: %w", err)
		}
		if endpoint, err = awsConf.FieldString("endpoint"); err != nil {
			return nil, err
		}
		region, _ = awsConf.FieldString("region")
		if region != "" {
			awsCfg.Region = region
		}
		if awsCfg.Region == "" {
			return nil, errors.New("aws.region is required for IAM authentication")
		}

		// tokenBuilder will be called upon component connection to refresh token/password and reconnect.
		// Tokens last ~15 minutes and will only need refreshing after a connection is lost.
		tokenBuilder := func(ctx context.Context) error {
			password, err := auth.BuildAuthToken(ctx, endpoint, awsCfg.Region, dbConf.User, awsCfg.Credentials)
			if err != nil {
				return fmt.Errorf("unable to build IAM auth token: %w", err)
			}
			dbConf.Password = password
			log.Debug("IAM authentication token generated successfully")
			return nil
		}
		return tokenBuilder, nil
	}
}
