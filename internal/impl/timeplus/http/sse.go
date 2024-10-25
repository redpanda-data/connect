package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type sseClient struct {
	header   http.Header
	queryURL *url.URL
	reader   *eventStreamReader
	cols     []col
	eventCH  chan []any
	readErr  error
	client   *http.Client
	logger   *service.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

type query struct {
	Result result `json:"result"`
}

type result struct {
	Header []col `json:"header"`
}

type col struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// NewSSEClient creates a Timeplus Enterprise SSE client.
// Since each SSE event could contain multiple messages, we should implement this as a BatchInput in the future.
func NewSSEClient(logger *service.Logger, baseURL *url.URL, workspace, apikey, username, password string) *sseClient {
	queryURL, _ := url.Parse(baseURL.String())

	queryURL.Path = path.Join(queryURL.Path, workspace, "api", timeplusAPIVersion, "queries")

	logger.With("host", queryURL.Host).With("query_url", queryURL.RequestURI()).Debug("new sse client created")

	return &sseClient{
		header:   NewHeader(apikey, username, password),
		queryURL: queryURL,
		eventCH:  make(chan []any),
		client:   newDefaultClient(),
		logger:   logger,
	}
}

func (c *sseClient) Run(sql string) error {
	payload := map[string]string{
		"sql": sql,
	}

	var body = new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(payload); err != nil {
		return err
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.queryURL.String(), body)
	if err != nil {
		return err
	}
	req.Header = c.header

	//nolint
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		resp.Body.Close()
		return fmt.Errorf("failed to run query, got status code %d", resp.StatusCode)
	}

	c.reader = newEventStreamReader(resp.Body, 1024*1024)
	cols, err := c.readQueryMeta()
	if err != nil {
		resp.Body.Close()
		return err
	}
	c.cols = cols

	go func() {
		defer func() {
			resp.Body.Close()
			close(c.eventCH)
		}()

		for {
			ev, err := c.reader.ReadEvent()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}

				c.readErr = err
				return
			}

			switch string(ev.Event) {
			case "":
				var events [][]any
				if err := json.Unmarshal(ev.Data, &events); err != nil {
					c.readErr = err
					return
				}

				for _, ev := range events {
					c.eventCH <- ev
				}
			default:
				continue
			}
		}
	}()

	return nil
}

func (c *sseClient) Read(ctx context.Context) (map[string]any, error) {
	if c.readErr != nil {
		return nil, c.readErr
	}

	select {
	case event, ok := <-c.eventCH:
		if !ok {
			return nil, nil
		}

		if len(event) != len(c.cols) {
			return nil, fmt.Errorf("rows in cols %d doesn't match cols in header %d", len(event), len(c.cols))
		}
		msg := map[string]any{}

		for i := range event {
			msg[c.cols[i].Name] = event[i]
		}

		return msg, nil
	case <-ctx.Done():
		return nil, nil
	default:
		return nil, c.readErr
	}
}

func (c *sseClient) Close(context.Context) error {
	c.cancel()

	return nil
}

func (c *sseClient) readQueryMeta() ([]col, error) {
	ev, err := c.reader.ReadEvent()
	if err != nil {
		return nil, err
	}

	if string(ev.Event) != "query" {
		return nil, fmt.Errorf("expect 'query', got %s", ev.Event)
	}

	q := query{}

	if err := json.Unmarshal(ev.Data, &q); err != nil {
		return nil, err
	}

	return q.Result.Header, nil
}
