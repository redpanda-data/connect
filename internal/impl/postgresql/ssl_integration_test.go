// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

type sslTestCerts struct {
	caCert     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

// generateCerts creates a temporary directory and generates a CA, server certificate/key, and client certificate/key for testing.
// It returns the paths to the generated files and a cleanup function.
func generateCerts(t *testing.T) (sslTestCerts, func()) {
	t.Helper()
	dir := t.TempDir()

	certs := sslTestCerts{}

	// --- Generate CA ---
	certs.caCert = filepath.Join(dir, "ca.crt")
	caKey := filepath.Join(dir, "ca.key")
	require.NoError(t, exec.Command("openssl", "genrsa", "-out", caKey, "2048").Run())
	require.NoError(t, exec.Command("openssl", "req", "-new", "-x509", "-sha256", "-days", "365", "-nodes", "-key", caKey, "-out", certs.caCert, "-subj", "/CN=MyTestCA").Run())

	// --- Generate Server Cert ---
	certs.serverCert = filepath.Join(dir, "server.crt")
	certs.serverKey = filepath.Join(dir, "server.key")
	serverCsr := filepath.Join(dir, "server.csr")
	v3Ext := filepath.Join(dir, "v3.ext")

	// Define the v3.ext content for SAN
	v3ExtData := `authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
`
	require.NoError(t, os.WriteFile(v3Ext, []byte(v3ExtData), 0o644))

	require.NoError(t, exec.Command("openssl", "genrsa", "-out", certs.serverKey, "2048").Run())
	require.NoError(t, exec.Command("openssl", "req", "-new", "-key", certs.serverKey, "-out", serverCsr, "-subj", "/CN=localhost").Run())
	require.NoError(t, exec.Command("openssl", "x509", "-req", "-in", serverCsr, "-CA", certs.caCert, "-CAkey", caKey, "-CAcreateserial", "-out", certs.serverCert, "-days", "365", "-sha256", "-extfile", v3Ext).Run())

	// --- Generate Client Cert ---
	certs.clientCert = filepath.Join(dir, "client.crt")
	certs.clientKey = filepath.Join(dir, "client.key")
	clientCsr := filepath.Join(dir, "client.csr")
	require.NoError(t, exec.Command("openssl", "genrsa", "-out", certs.clientKey, "2048").Run())
	require.NoError(t, exec.Command("openssl", "req", "-new", "-key", certs.clientKey, "-out", clientCsr, "-subj", "/CN=testuser").Run())
	require.NoError(t, exec.Command("openssl", "x509", "-req", "-in", clientCsr, "-CA", certs.caCert, "-CAkey", caKey, "-CAcreateserial", "-out", certs.clientCert, "-days", "365", "-sha256").Run())

	// Return the cert paths and a cleanup function
	return certs, func() {}
}

func resourceWithPostgreSQLVersionSSL(t *testing.T, version string, certs sslTestCerts, clientAuth string) (testcontainers.Container, *sql.DB) {
	t.Helper()

	pgHbaContent := `
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
`
	if clientAuth != "" {
		pgHbaContent = fmt.Sprintf(`
hostssl all all all cert clientcert=%s
`, clientAuth)
	}

	ctr, err := testcontainers.Run(t.Context(), "postgres:"+version,
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": "l]YLSc|4[i56_@{gY",
			"POSTGRES_USER":     "testuser",
			"POSTGRES_DB":       "dbname",
		}),
		// Override entrypoint to chown SSL cert files before starting postgres.
		// WithFiles copies files as root; postgres requires the key to be owned
		// by the postgres user. The wrapper chowns the files and then delegates
		// to the original entrypoint.
		testcontainers.WithEntrypoint(
			"bash", "-c",
			`chown postgres:postgres /var/lib/postgresql/server.crt /var/lib/postgresql/server.key /var/lib/postgresql/ca.crt && chmod 600 /var/lib/postgresql/server.key && exec docker-entrypoint.sh "$@"`,
			"--",
		),
		testcontainers.WithCmd(
			"postgres",
			"-c", "wal_level=logical",
			"-c", "ssl=on",
			"-c", "ssl_cert_file=/var/lib/postgresql/server.crt",
			"-c", "ssl_key_file=/var/lib/postgresql/server.key",
			"-c", "ssl_ca_file=/var/lib/postgresql/ca.crt",
		),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{
				HostFilePath:      certs.serverCert,
				ContainerFilePath: "/var/lib/postgresql/server.crt",
				FileMode:          0o644,
			},
			testcontainers.ContainerFile{
				HostFilePath:      certs.serverKey,
				ContainerFilePath: "/var/lib/postgresql/server.key",
				FileMode:          0o600,
			},
			testcontainers.ContainerFile{
				HostFilePath:      certs.caCert,
				ContainerFilePath: "/var/lib/postgresql/ca.crt",
				FileMode:          0o644,
			},
		),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	// Overwrite pg_hba.conf to enforce SSL and reload PostgreSQL config.
	require.Eventually(t, func() bool {
		exitCode, out, err := ctr.Exec(t.Context(), []string{
			"bash", "-c",
			fmt.Sprintf("echo '%s' > /var/lib/postgresql/data/pg_hba.conf", pgHbaContent),
		})
		_, _ = io.Copy(io.Discard, out)
		if err != nil || exitCode != 0 {
			return false
		}
		exitCode, out, err = ctr.Exec(t.Context(), []string{
			"su", "postgres", "-c",
			"pg_ctl reload -D /var/lib/postgresql/data",
		})
		_, _ = io.Copy(io.Discard, out)
		return err == nil && exitCode == 0
	}, 30*time.Second, time.Second, "exhausted retries updating container configuration")

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)

	dsn := fmt.Sprintf(
		"user=testuser password='l]YLSc|4[i56_@{gY' dbname=dbname sslmode=disable host=%s port=%s",
		host, mp.Port(),
	)

	var db *sql.DB
	require.Eventually(t, func() bool {
		if db != nil {
			db.Close()
		}
		var dbErr error
		db, dbErr = sql.Open("postgres", dsn)
		if dbErr != nil {
			return false
		}
		if db.Ping() != nil {
			db.Close()
			db = nil
			return false
		}
		return true
	}, time.Minute, time.Second)

	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, content VARCHAR(50));")
	require.NoError(t, err)

	return ctr, db
}

func TestIntegrationSSLVerifyFull(t *testing.T) {
	// This test appears to constantly fail in CI only, looks to be related to
	// setting the SSL certs in the container in resourceWithPostgreSQLVersionSSL.
	if os.Getenv("CI") != "" {
		t.Skip("Skipping test in CI")
	}

	integration.CheckSkip(t)

	certs, cleanup := generateCerts(t)
	defer cleanup()

	ctr, db := resourceWithPostgreSQLVersionSSL(t, "16", certs, "1")

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)

	caCertContent, err := os.ReadFile(certs.caCert)
	require.NoError(t, err)
	clientCertContent, err := os.ReadFile(certs.clientCert)
	require.NoError(t, err)
	clientKeyContent, err := os.ReadFile(certs.clientKey)
	require.NoError(t, err)

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: "host=%s port=%s user=testuser password='l]YLSc|4[i56_@{gY' dbname=dbname sslmode=verify-full"
    slot_name: test_slot_ssl
    stream_snapshot: true
    schema: public
    tables:
       - test_table
    tls:
      root_cas: |
%s
      client_certs:
        - cert: |
%s
          key: |
%s
`,
		host,
		mp.Port(),
		indent(string(caCertContent), 8),
		indent(string(clientCertContent), 12),
		indent(string(clientKeyContent), 12),
	)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		for _, msg := range mb {
			msgBytes, err := msg.AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(streamOut.Resources())

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	_, err = db.Exec("INSERT INTO test_table (content) VALUES ('hello world base64');")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1
	}, time.Second*30, time.Second, "timed out waiting for snapshot message")

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

func indent(s string, spaces int) string {
	var builder strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		builder.WriteString(strings.Repeat(" ", spaces))
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	return builder.String()
}
