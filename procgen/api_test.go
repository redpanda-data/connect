package procgen

import (
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/procgen/api"
)

type tokenSource struct {
	token string
}

func (t *tokenSource) BearerAuth(_ context.Context, _ api.OperationName) (api.BearerAuth, error) {
	return api.BearerAuth{Token: t.token}, nil
}

func TestV1UsersGet(t *testing.T) {
	client, err := api.NewClient("https://api.notion.com", &tokenSource{token: "ntn_b89409055012xxojYsLeGAGwziAEEHbClkNcl302Q0K8ZG"})
	require.NoError(t, err)

	resp, err := client.V1UsersGet(context.Background(), api.V1UsersGetParams{
		NotionVersion: api.NewOptString("2022-06-28"),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	res
	b, err := io.ReadAll(resp
	t.Log(resp)
}
