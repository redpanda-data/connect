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

const secretPrefix = "secrets."

type secretAPI interface {
	getSecretValue(string) (string, bool)
}

type createSecretsManagerFn func(ctx context.Context, logger *slog.Logger, url *url.URL) (secretAPI, error)

type secretManager struct {
	secretAPI secretAPI
	prefix    string
}

func (s *secretManager) lookup(_ context.Context, key string) (string, bool) {
	if !strings.HasPrefix(key, secretPrefix) {
		return "", false
	}
	key = strings.TrimPrefix(key, secretPrefix)
	parts := strings.SplitN(key, ".", 2)

	secretName := s.prefix + parts[0]
	value, found := s.secretAPI.getSecretValue(secretName)
	if !found {
		return "", false
	}

	if len(parts) == 1 {
		return value, true
	}

	return getJsonValue(value, parts[1])
}

func newSecretManager(ctx context.Context, logger *slog.Logger, url *url.URL, createSecretsManagerFn createSecretsManagerFn) (LookupFn, error) {
	secretsManager, err := createSecretsManagerFn(ctx, logger, url)
	if err != nil {
		return nil, err
	}
	secretManager := &secretManager{
		secretAPI: secretsManager,
		prefix:    trimLeftSlash(url.Path),
	}

	return secretManager.lookup, nil
}

func trimLeftSlash(path string) string {
	return strings.TrimPrefix(path, "/")
}

func getJsonValue(json string, field string) (string, bool) {
	result := gjson.Get(json, field)
	return result.String(), result.Exists()
}
