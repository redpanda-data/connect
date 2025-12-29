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

package kubernetes

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestBuildKubeconfigUsesEnv(t *testing.T) {
	dir := t.TempDir()
	server := "https://env-cluster.example.com:6443"
	kubeconfigPath := writeKubeconfig(t, dir, server)

	t.Setenv("KUBECONFIG", kubeconfigPath)

	conf := parseAuthConfig(t, `
auto_auth: false
`)

	config, err := buildKubeconfigClient(conf)
	require.NoError(t, err)
	require.Equal(t, server, config.Host)
}

func TestBuildKubeconfigExplicitOverridesEnv(t *testing.T) {
	dir := t.TempDir()

	envServer := "https://env-cluster.example.com:6443"
	envPath := writeKubeconfig(t, dir, envServer)
	t.Setenv("KUBECONFIG", envPath)

	explicitServer := "https://explicit-cluster.example.com:6443"
	explicitPath := writeKubeconfig(t, dir, explicitServer)

	conf := parseAuthConfig(t, fmt.Sprintf(`
auto_auth: false
kubeconfig: %s
`, explicitPath))

	config, err := buildKubeconfigClient(conf)
	require.NoError(t, err)
	require.Equal(t, explicitServer, config.Host)
}

func parseAuthConfig(t *testing.T, yamlConfig string) *service.ParsedConfig {
	t.Helper()

	spec := service.NewConfigSpec().Fields(AuthFields()...)
	conf, err := spec.ParseYAML(yamlConfig, nil)
	require.NoError(t, err)
	return conf
}

func writeKubeconfig(t *testing.T, dir, server string) string {
	t.Helper()

	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: test-cluster
  cluster:
    server: %s
    insecure-skip-tls-verify: true
contexts:
- name: test-context
  context:
    cluster: test-cluster
    user: test-user
current-context: test-context
users:
- name: test-user
  user:
    token: test-token
`, server)

	filename := sanitizeServer(server) + ".kubeconfig"
	path := filepath.Join(dir, filename)
	err := os.WriteFile(path, []byte(kubeconfig), 0o600)
	require.NoError(t, err)
	return path
}

func sanitizeServer(server string) string {
	result := make([]byte, 0, len(server))
	for i := 0; i < len(server); i++ {
		c := server[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	return string(result)
}
