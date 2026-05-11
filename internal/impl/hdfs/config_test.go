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

package hdfs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestHDFSOutputConfigParsing(t *testing.T) {
	env := service.NewEnvironment()

	tests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "old config without auth",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
path: ${!counter()}.txt
`,
		},
		{
			name: "kerberos disabled does not require credentials",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
auth:
  kerberos:
    enabled: false
`,
		},
		{
			name: "kerberos enabled",
			config: `
hosts: [namenode.example.com:8020]
user: connect
directory: /events
auth:
  kerberos:
    enabled: true
    krb5_conf: /etc/krb5.conf
    keytab: /etc/security/keytabs/connect.keytab
    principal: connect/worker@EXAMPLE.COM
    service_principal: nn/_HOST
    data_transfer_protection: privacy
`,
		},
		{
			name: "kerberos missing keytab",
			config: `
hosts: [localhost:9000]
directory: /tmp
auth:
  kerberos:
    enabled: true
    principal: connect/worker@EXAMPLE.COM
`,
			errContains: "auth.kerberos.keytab is required",
		},
		{
			name: "kerberos missing principal",
			config: `
hosts: [localhost:9000]
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
`,
			errContains: "auth.kerberos.principal is required",
		},
		{
			name: "kerberos invalid principal",
			config: `
hosts: [localhost:9000]
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect
`,
			errContains: "auth.kerberos.principal is invalid",
		},
		{
			name: "invalid data transfer protection",
			config: `
hosts: [localhost:9000]
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
    data_transfer_protection: invalid
`,
			errContains: "auth.kerberos.data_transfer_protection",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := outputSpec().ParseYAML(test.config, env)
			require.NoError(t, err)
			_, err = hdfsConfigFromParsed(parsed)
			if test.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.errContains)
		})
	}
}

func TestHDFSInputConfigParsing(t *testing.T) {
	env := service.NewEnvironment()

	tests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "old config without auth",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
`,
		},
		{
			name: "kerberos enabled with empty data transfer protection",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
`,
		},
		{
			name: "kerberos enabled with authentication",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
    data_transfer_protection: authentication
`,
		},
		{
			name: "kerberos enabled with integrity",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
    data_transfer_protection: integrity
`,
			errContains: `auth.kerberos.data_transfer_protection "integrity" is not supported for the hdfs input`,
		},
		{
			name: "kerberos enabled with privacy",
			config: `
hosts: [localhost:9000]
user: root
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
    data_transfer_protection: privacy
`,
			errContains: `auth.kerberos.data_transfer_protection "privacy" is not supported for the hdfs input`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := inputSpec().ParseYAML(test.config, env)
			require.NoError(t, err)
			conf, err := hdfsInputConfigFromParsed(parsed)
			if test.errContains == "" {
				require.NoError(t, err)
				require.Equal(t, []string{"localhost:9000"}, conf.hosts)
				require.Equal(t, "root", conf.user)
				return
			}
			require.ErrorContains(t, err, test.errContains)
		})
	}
}

func TestHDFSKerberosConfigDefaults(t *testing.T) {
	env := service.NewEnvironment()

	parsed, err := outputSpec().ParseYAML(`
hosts: [localhost:9000]
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
`, env)
	require.NoError(t, err)

	conf, err := hdfsConfigFromParsed(parsed)
	require.NoError(t, err)
	require.Equal(t, defaultKerberosKrb5Conf, conf.kerberos.krb5Conf)
	require.Equal(t, defaultKerberosServicePrincipal, conf.kerberos.servicePrincipal)
	require.Empty(t, conf.kerberos.dataTransferProtection)
}

func TestHDFSOutputConfigAcceptsAllDataTransferProtectionModes(t *testing.T) {
	env := service.NewEnvironment()

	for _, mode := range []string{
		dataTransferProtectionAuthentication,
		dataTransferProtectionIntegrity,
		dataTransferProtectionPrivacy,
	} {
		t.Run(mode, func(t *testing.T) {
			parsed, err := outputSpec().ParseYAML(`
hosts: [localhost:9000]
directory: /tmp
auth:
  kerberos:
    enabled: true
    keytab: /tmp/connect.keytab
    principal: connect/worker@EXAMPLE.COM
    data_transfer_protection: `+mode+`
`, env)
			require.NoError(t, err)

			conf, err := hdfsConfigFromParsed(parsed)
			require.NoError(t, err)
			require.Equal(t, mode, conf.kerberos.dataTransferProtection)
		})
	}
}
