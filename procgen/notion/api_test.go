package notion

import (
	"context"
	"testing"

	nv1 "github.com/redpanda-data/connect/v4/procgen/notion/api/v1"
	"github.com/stretchr/testify/require"
)

type tokenSource struct {
	token string
}

func (t *tokenSource) BearerAuth(_ context.Context, _ nv1.OperationName) (nv1.BearerAuth, error) {
	return nv1.BearerAuth{Token: t.token}, nil
}

func TestUsersGet(t *testing.T) {
	client, err := nv1.NewClient("https://api.notion.com", &tokenSource{token: "ntn_b89409055012xxojYsLeGAGwziAEEHbClkNcl302Q0K8ZG"})
	require.NoError(t, err)

	resp, err := client.UsersGet(context.Background(), nv1.UsersGetParams{
		NotionVersion: nv1.NewOptString("2022-06-28"),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Log(resp.Response.Validate())
	t.Log(resp.Response)
}
