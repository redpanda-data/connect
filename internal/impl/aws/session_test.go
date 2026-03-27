package aws

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

func TestGetSessionWithProvidedBundle(t *testing.T) {
	const dummyCACert = `-----BEGIN CERTIFICATE-----
MIIDazCCAlOgAwIBAgIUFs1+VvPMZdXUuqmUlDhQJan9cOEwDQYJKoZIhvcNAQEL
BQAwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yNjAzMTExNzI5MTJaFw0yNjA0
MTAxNzI5MTJaMEUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw
HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDFr1bf2m3kBiv7cNShG++nrTXE2RJgkLN1d/XVIUND
Wxd5vYkwyveOMKYweM6JEGs6JYfyGsSy4uupWk/ySmQf7aCbE74z6zLTB5xThuix
h9WhYjqdwUBJfuPeVQ5Gwc25pWer1hJSuqi6XUtzVFqpSfNRrJaRkWFL+4tMMdWF
f5u2ipt6o55+FJHaBtN3QT/JhZWx8S5IyOwPVbdLcF/PMQ+JuFXYDDz/5h1ZnacR
M7ULCUseI4u0wjBeIYP3oi7pCKQbTp+duKoRjmrtmETL441oGquLJmfQyJ9tZbFz
EF7bTRSVyKBqnpwz4sa4dCIqJW6VlWpjCmpmqvTDX1a3AgMBAAGjUzBRMB0GA1Ud
DgQWBBSl4ToCW6+nqjeFup7FSTPs8/Ek7zAfBgNVHSMEGDAWgBSl4ToCW6+nqjeF
up7FSTPs8/Ek7zAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBF
OcFMYf1gIjU2c2LEi5/3uiBaza5O5ymm9BCdvBku9ClJWEXVgnBxvOWAI2dHCDq4
zYE33/fMiKXhYHbVMicjwR5WGRc2dUyegKd98YLPJf6Trup3X8MUVfzEsvecwYOr
I3iVNK1rpVNs8XQBK4sf4eImBiFkfufjcZ3xx/NxenJI9MmEhqVo6IMNhfh7aXaa
xAXpC7KSbOtqUPmj4F+0afW6HpRsNi7lxPqOq63Cz7Tys7kBqmZQB+3zKMd2CGX7
X6lhxAUqZLVwp5lUgdX2O0z9U64xJ+6YC8qyKj1uBYb0qk9cUEC5obh7madnslJQ
JIK5fyYtkm94tgnoglCV
-----END CERTIFICATE-----
`

	caPath := filepath.Join(t.TempDir(), "dummy_ca.crt")
	require.NoError(t, os.WriteFile(caPath, []byte(dummyCACert), 0o600))
	t.Setenv("AWS_CA_BUNDLE", caPath)
	conf := `
tcp:
  connect_timeout: 2s
`
	spec := service.NewConfigSpec().
		Categories("Services", "AWS").
		Fields(config.SessionFields()...)
	pConf, err := spec.ParseYAML(conf, nil)
	require.NoError(t, err)
	_, err = GetSession(context.TODO(), pConf)
	assert.NoError(t, err)
}
