package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/semaphore"
)

const (
	timeplusAPIVersion          = "v1beta2"
	timeplusdDAPIVersion        = "v1"
	TargetTimeplus       string = "timeplus"
	TargetTimeplusd      string = "timeplusd"
)

type Client struct {
	logger    *service.Logger
	ingestURL *url.URL
	header    http.Header
	client    *http.Client
	sem       *semaphore.Weighted
}

type tpIngest struct {
	Columns []string `json:"columns" binding:"required"`
	Data    [][]any  `json:"data" binding:"required"`
}

func NewClient(logger *service.Logger, maxInFlight int, target string, baseURL *url.URL, workspace, stream, apikey, username, password string) *Client {
	ingestURL, _ := url.Parse(baseURL.String())

	header := http.Header{}
	header.Add("Content-Type", "application/json")

	if len(username)+len(password) > 0 {
		auth := username + ":" + password
		header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))

		logger = logger.With("auth_method", "basic")
	}

	if target == TargetTimeplus {
		ingestURL.Path = path.Join(ingestURL.Path, workspace, "api", timeplusAPIVersion, "streams", stream, "ingest")

		if len(apikey) > 0 {
			header.Add("X-Api-Key", apikey)
			logger = logger.With("auth_method", "apikey")
		}
	} else if target == TargetTimeplusd {
		ingestURL.Path = path.Join(ingestURL.Path, "timeplusd", timeplusdDAPIVersion, "ingest", "streams", stream)
	}

	logger = logger.With("target", TargetTimeplusd).With("host", ingestURL.Host).With("ingest_url", ingestURL.RequestURI())
	logger.Info("timeplus http client created")

	return &Client{
		logger,
		ingestURL,
		header,
		&http.Client{},
		semaphore.NewWeighted(int64(maxInFlight)),
	}
}

func (c *Client) Write(ctx context.Context, cols []string, rows [][]any) error {
	payload := tpIngest{
		Columns: cols,
		Data:    rows,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, c.ingestURL.String(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	req.Header = c.header

	if err := c.sem.Acquire(ctx, 1); err != nil {
		return err
	}

	defer c.sem.Release(1)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		errorBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to ingest, got status code %d", resp.StatusCode)
		}

		return fmt.Errorf("failed to ingest, got status code %d, error %s", resp.StatusCode, errorBody)
	}

	return nil
}
