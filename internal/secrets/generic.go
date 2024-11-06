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

package secrets

import (
	"context"
	"log/slog"
	"net/url"
	"strings"

	"github.com/tidwall/gjson"
)

// prefix used to reference secrets from external secret managers, to differentiate them from environment variables
const secretPrefix = "secrets."

type secretAPI interface {
	getSecretValue(context.Context, string) (string, bool)
	checkSecretExists(context.Context, string) bool
}

type createSecretsManagerFn func(ctx context.Context, logger *slog.Logger, url *url.URL) (secretAPI, error)

type secretManager struct {
	secretAPI secretAPI
	prefix    string
}

func (s *secretManager) lookup(ctx context.Context, key string) (string, bool) {
	secretName, field, ok := s.trimPrefixAndSplit(key)
	if !ok {
		return "", false
	}

	value, found := s.secretAPI.getSecretValue(ctx, secretName)
	if !found {
		return "", false
	}

	if field == "" {
		return value, true
	}

	return getJSONValue(value, field)
}

func (s *secretManager) exists(ctx context.Context, key string) bool {
	secretName, _, ok := s.trimPrefixAndSplit(key)
	if !ok {
		return false
	}

	return s.secretAPI.checkSecretExists(ctx, secretName)
}

func newSecretManager(ctx context.Context, logger *slog.Logger, url *url.URL, createSecretsManagerFn createSecretsManagerFn) (LookupFn, ExistsFn, error) {
	secretsManager, err := createSecretsManagerFn(ctx, logger, url)
	if err != nil {
		return nil, nil, err
	}
	secretManager := &secretManager{
		secretAPI: secretsManager,
		prefix:    strings.TrimPrefix(url.Path, "/"),
	}

	return secretManager.lookup, secretManager.exists, nil
}

// trims the secret prefix and returns full secret ID with JSON field reference
func (s *secretManager) trimPrefixAndSplit(key string) (string, string, bool) {
	if !strings.HasPrefix(key, secretPrefix) {
		return "", "", false
	}

	key = strings.TrimPrefix(key, secretPrefix)
	if strings.Contains(key, ".") {
		parts := strings.SplitN(key, ".", 2)
		return s.prefix + parts[0], parts[1], true
	}

	return s.prefix + key, "", true
}

func getJSONValue(json string, field string) (string, bool) {
	result := gjson.Get(json, field)
	return result.String(), result.Exists()
}
