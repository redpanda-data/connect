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

//go:build arm

package sql

import (
	"crypto/rsa"
	"database/sql"
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// snowflakeAuthFields returns empty config fields on ARM (Snowflake not supported).
func snowflakeAuthFields() []*service.ConfigField {
	return nil
}

// snowflakeAuthConfig holds the parsed Snowflake authentication configuration.
type snowflakeAuthConfig struct {
	privateKey     *rsa.PrivateKey
	hasKeyPairAuth bool
}

// parseSnowflakeAuthConfig returns nil on ARM (Snowflake not supported).
func parseSnowflakeAuthConfig(_ *service.ParsedConfig, _ *service.Resources) (*snowflakeAuthConfig, error) {
	return nil, nil
}

// openSnowflakeWithKeyPair is not supported on ARM.
func openSnowflakeWithKeyPair(_ string, _ *rsa.PrivateKey) (*sql.DB, error) {
	return nil, errors.New("snowflake key pair authentication is not supported on ARM architecture")
}

