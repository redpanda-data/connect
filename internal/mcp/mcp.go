package mcp

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"

	"github.com/mark3labs/mcp-go/server"

	"github.com/redpanda-data/connect/v4/internal/mcp/repository"
	"github.com/redpanda-data/connect/v4/internal/mcp/tools"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// Run an mcp server against a target directory, with an optional base URL for
// an HTTP server.
func Run(logger *slog.Logger, repositoryDir, baseURLStr string) error {
	// Create MCP server
	s := server.NewMCPServer(
		"Redpanda Runtime",
		"1.0.0",
	)

	resWrapper := tools.NewResourcesWrapper(logger, s)

	repoScanner := repository.NewScanner(os.DirFS(repositoryDir))
	repoScanner.OnResourceFile(func(resourceType string, filename string, contents []byte) error {
		switch resourceType {
		case "cache":
			if err := resWrapper.AddCache(contents); err != nil {
				return err
			}
		case "processor":
			if err := resWrapper.AddProcessor(contents); err != nil {
				return err
			}
		default:
			return fmt.Errorf("resource type '%v' is not supported yet", resourceType)
		}
		return nil
	})
	if err := repoScanner.Scan("."); err != nil {
		return err
	}

	if err := resWrapper.Build(); err != nil {
		return err
	}

	if baseURLStr != "" {
		baseURL, err := url.Parse(baseURLStr)
		if err != nil {
			return err
		}

		sseServer := server.NewSSEServer(s, server.WithBaseURL(baseURLStr))
		logger.Info("SSE server listening")
		if err := sseServer.Start(":" + baseURL.Port()); err != nil {
			return err
		}
	} else {
		if err := server.ServeStdio(s); err != nil {
			return err
		}
	}

	return nil
}
