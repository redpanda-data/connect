package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestVaultKey(t *testing.T) {
	t.Parallel()

	loggedIn := false

	sm := http.NewServeMux()
	sm.HandleFunc("/v1/auth/approle_foo/login", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]any{}

		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&m)
		require.NoError(t, err)

		roleID := m["role_id"].(string)
		secretID := m["secret_id"].(string)

		require.Equal(t, "magic", roleID)
		require.Equal(t, "beans", secretID)

		loggedIn = true

		_, err = io.WriteString(w, `
{
  "auth": {
    "renewable": true,
    "lease_duration": 1200,
    "metadata": null,
    "token_policies": ["default"],
    "accessor": "fd6c9a00-d2dc-3b11-0be5-af7ae0e1d374",
    "client_token": "5b1a0318-679c-9c45-e5c6-d1b9a9035d49"
  },
  "warnings": null,
  "wrap_info": null,
  "data": null,
  "lease_duration": 0,
  "renewable": false,
  "lease_id": ""
}
`)
		require.NoError(t, err)
	})
	sm.HandleFunc("/v1/rico/data/dev%2Ffoo%2Fother", func(w http.ResponseWriter, r *http.Request) {
		require.True(t, loggedIn)

		version := r.URL.Query().Get("version")
		require.Equal(t, "9", version)

		_, err := io.WriteString(w, `
{
  "data": {
    "data": {
      "foo": "bar"
    },
    "metadata": {
      "created_time": "2018-03-22T02:24:06.945319214Z",
      "custom_metadata": {
        "owner": "jdoe",
        "mission_critical": "false"
      },
      "deletion_time": "",
      "destroyed": false,
      "version": 9
    }
  }
}
`)
		require.NoError(t, err)
	})

	testServer := httptest.NewServer(sm)
	defer testServer.Close()

	pc, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
auth:
  mount_path: approle_foo
  app_role:
    role_id: "magic"
    secret_id: "beans"
path: |
  root = "dev/foo/other"
version: ${! metadata("keyversion") }
mount_path: rico
`, testServer.URL), nil)
	require.NoError(t, err)

	res := service.MockResources()

	ctx := context.Background()

	proc, err := ctor(pc, res)
	require.NoError(t, err)
	defer require.NoError(t, proc.Close(ctx))

	msg := service.NewMessage([]byte("keya"))
	msg.MetaSet("keyversion", "9")
	resBatch, err := proc.Process(ctx, msg)
	require.NoError(t, err)
	require.Len(t, resBatch, 1)

	resMsg := resBatch[0]
	require.NoError(t, resMsg.GetError())

	gotten, err := resMsg.AsStructured()
	require.NoError(t, err)

	expected := map[string]any{"foo": "bar"}

	require.Equal(t, expected, gotten)
}
