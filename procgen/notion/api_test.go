package notion

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/procgen/notion/notionapi"
)

type tokenSource struct {
	token string
}

func (t *tokenSource) BearerAuth(_ context.Context, _ notionapi.OperationName) (notionapi.BearerAuth, error) {
	return notionapi.BearerAuth{Token: t.token}, nil
}

func TestV1UsersGet(t *testing.T) {
	client, err := notionapi.NewClient("https://api.notion.com", &tokenSource{token: "ntn_b89409055012xxojYsLeGAGwziAEEHbClkNcl302Q0K8ZG"})
	require.NoError(t, err)

	resp, err := client.V1UsersGet(context.Background(), notionapi.V1UsersGetParams{
		NotionVersion: notionapi.NewOptString("2022-06-28"),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Log(resp.Response.Validate())
	t.Log(resp.Response)
}
