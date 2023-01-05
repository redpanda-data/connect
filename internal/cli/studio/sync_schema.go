package studio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/urfave/cli/v2"

	"github.com/benthosdev/benthos/v4/internal/config/schema"
)

func syncSchemaCommand(version, dateBuilt string) *cli.Command {
	return &cli.Command{
		Name:  "sync-schema",
		Usage: "Synchronizes the schema of this Benthos instance with a studio session",
		Description: `
This sync allows custom plugins and templates to be configured and linted
correctly within Benthos studio.

In order to synchronize a single use token must be generated from the session
page within the studio application.`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "session",
				Aliases:  []string{"s"},
				Required: true,
				Value:    "",
				Usage:    "The session ID to synchronize with.",
			},
			&cli.StringFlag{
				Name:     "token",
				Aliases:  []string{"t"},
				Required: true,
				Value:    "",
				Usage:    "The single use token used to authenticate the request.",
			},
		},
		Action: func(c *cli.Context) error {
			endpoint := c.String("endpoint")
			sessionID := c.String("session")
			tokenID := c.String("token")

			u, err := url.Parse(endpoint)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to parse endpoint: %v\n", err)
				os.Exit(1)
			}
			u.Path = path.Join(u.Path, fmt.Sprintf("/api/v1/token/%v/session/%v/schema", tokenID, sessionID))

			schema := schema.New(version, dateBuilt)
			schema.Scrub()
			schemaBytes, err := json.Marshal(schema)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to encode schema: %v\n", err)
				os.Exit(1)
			}

			res, err := http.Post(u.String(), "application/json", bytes.NewReader(schemaBytes))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Sync request failed: %v\n", err)
				os.Exit(1)
			}

			if res.StatusCode < 200 || res.StatusCode > 299 {
				resBytes, _ := io.ReadAll(res.Body)
				fmt.Fprintf(os.Stderr, "Sync request failed (%v): %v\n", res.StatusCode, string(resBytes))
				os.Exit(1)
			}
			return nil
		},
	}
}
