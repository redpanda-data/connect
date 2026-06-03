// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jira

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/jiraauth"
	"github.com/redpanda-data/connect/v4/internal/impl/jira/jirahttp"
)

// cursorSchemaVersion is the on-disk format version stamped into the cursor
// JSON; it is consumed by writeCursor when the input advances the cursor.
const cursorSchemaVersion = 1

const (
	resourceIssues    = "issues"
	resourceComments  = "comments"
	resourceChangelog = "changelog"
)

var validResources = []string{resourceIssues, resourceComments, resourceChangelog}

// cursor is the persisted incremental-fetch checkpoint for the jira input.
// It stores the max issue.updated timestamp seen in the last fully-acked page.
// Unknown JSON fields are ignored on decode for forward compatibility.
type cursor struct {
	Updated time.Time `json:"updated"`
	Version int       `json:"v"`
}

func newJiraInputConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Version("4.95.0").
		Summary("Streams Jira issues, comments, or changelog entries via JQL with incremental polling.").
		Description(`Periodically queries Jira's REST API using a JQL filter and emits one message per resource. The cursor (max issue ` + "`updated`" + ` timestamp) is persisted via the configured cache resource so progress survives restarts.

Authentication uses API token (email + token) basic auth.

Each message body is the raw JSON of the resource. Metadata fields:

- ` + "`jira_id`" + ` - issue key (issues) / comment ID / changelog history ID
- ` + "`jira_issue_key`" + ` - parent issue key (omitted for resource=issues)
- ` + "`jira_project`" + ` - project key
- ` + "`jira_updated`" + ` - RFC 3339 timestamp of the resource
- ` + "`jira_event_type`" + ` - "issue" / "comment" / "changelog"
- ` + "`jira_self`" + ` - Jira API URL of the resource

Limitations (v1): OAuth and the worklogs resource are not yet supported. For resource=comments and resource=changelog, only the first page of child resources (up to ~50 comments or ~100 changelog entries per issue update) is emitted; a WARN is logged when truncation is detected. Use a downstream Jira processor to fetch the full child set if your issues exceed this limit.`).
		Field(service.NewObjectField("auth",
			service.NewStringField("email").
				Description("Email or username of the Jira account."),
			service.NewStringField("api_token").
				Description("Jira API token.").
				Secret(),
		).Description("API token authentication.")).
		Field(service.NewStringEnumField("resource", validResources...).
			Description("Which Jira resource to emit.").
			Default(resourceIssues)).
		Field(service.NewStringField("jql").
			Description("Jira JQL filter. The input appends an `updated >= cursor` predicate and `ORDER BY updated ASC, key ASC`. Empty means all issues visible to the principal.").
			Default("")).
		Field(service.NewStringListField("fields").
			Description("Jira `fields` query parameter - narrow this for throughput.").
			Default([]any{"*all"})).
		Field(service.NewStringListField("expand").
			Description("Jira `expand` query parameter. The input automatically adds `changelog` when resource=changelog.").
			Default([]any{})).
		Field(service.NewIntField("page_size").
			Description("Issues per Jira page (Jira max 100).").
			Default(50)).
		Field(service.NewDurationField("poll_interval").
			Description("Time to wait between polls once the input has caught up. Minimum 10s.").
			Default("60s")).
		Field(service.NewObjectField("cursor",
			service.NewStringField("cache").
				Description("Name of a cache resource used to persist the cursor."),
			service.NewStringField("key").
				Description("Cache key. Defaults to `redpanda_connect_jira_input_<resource>`.").
				Default(""),
			service.NewDurationField("overlap").
				Description("Widens `updated >= cursor - overlap` to absorb minute-boundary effects. Jira JQL's `updated` operator has minute precision, so this should be set to at least 1m to have an effect.").
				Default("60s"),
		).Description("Cursor checkpoint storage.")).
		Field(service.NewAutoRetryNacksToggleField())

	spec.Fields(httpclient.FieldsWithBaseURL("")...)
	return spec
}

// inputCfg holds the parsed configuration for the jira input.
type inputCfg struct {
	httpCfg      *httpclient.Config
	authEmail    string
	authAPIToken string
	resource     string
	jql          string
	fields       []string
	expand       []string
	pageSize     int
	pollInterval time.Duration

	cacheName     string
	cacheKey      string
	cursorOverlap time.Duration
}

func parseInputConfig(conf *service.ParsedConfig) (*inputCfg, error) {
	httpCfg, err := httpclient.NewConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	email, err := conf.FieldString("auth", "email")
	if err != nil {
		return nil, err
	}
	if email == "" {
		return nil, errors.New("auth.email must not be empty")
	}
	apiToken, err := conf.FieldString("auth", "api_token")
	if err != nil {
		return nil, err
	}
	if apiToken == "" {
		return nil, errors.New("auth.api_token must not be empty")
	}

	resource, err := conf.FieldString("resource")
	if err != nil {
		return nil, err
	}
	if !isValidResource(resource) {
		if resource == "worklogs" {
			return nil, fmt.Errorf("resource %q is not supported in v1: the worklogs resource is not yet implemented", resource)
		}
		return nil, fmt.Errorf("resource %q is not valid; expected one of %v", resource, validResources)
	}

	jql, err := conf.FieldString("jql")
	if err != nil {
		return nil, err
	}
	fields, err := conf.FieldStringList("fields")
	if err != nil {
		return nil, err
	}
	expand, err := conf.FieldStringList("expand")
	if err != nil {
		return nil, err
	}
	pageSize, err := conf.FieldInt("page_size")
	if err != nil {
		return nil, err
	}
	if pageSize <= 0 || pageSize > 100 {
		return nil, errors.New("page_size must be between 1 and 100")
	}
	pollInterval, err := conf.FieldDuration("poll_interval")
	if err != nil {
		return nil, err
	}
	if pollInterval < 10*time.Second {
		return nil, errors.New("poll_interval must be at least 10s")
	}

	cacheName, err := conf.FieldString("cursor", "cache")
	if err != nil {
		return nil, err
	}
	if cacheName == "" {
		return nil, errors.New("cursor.cache must not be empty")
	}
	cacheKey, err := conf.FieldString("cursor", "key")
	if err != nil {
		return nil, err
	}
	if cacheKey == "" {
		cacheKey = "redpanda_connect_jira_input_" + resource
	}
	overlap, err := conf.FieldDuration("cursor", "overlap")
	if err != nil {
		return nil, err
	}

	return &inputCfg{
		httpCfg:       &httpCfg,
		authEmail:     email,
		authAPIToken:  apiToken,
		resource:      resource,
		jql:           jql,
		fields:        fields,
		expand:        expand,
		pageSize:      pageSize,
		pollInterval:  pollInterval,
		cacheName:     cacheName,
		cacheKey:      cacheKey,
		cursorOverlap: overlap,
	}, nil
}

func isValidResource(r string) bool {
	return slices.Contains(validResources, r)
}

// reader is the jira input implementation.
type reader struct {
	cfg    *inputCfg
	mgr    *service.Resources
	log    *service.Logger
	client *jirahttp.Client

	// metrics.
	cacheSetErrors *service.MetricCounter
	// childTruncated counts how many per-issue child fetches returned fewer
	// rows than the server-reported total. Labelled by resource ("comments"
	// or "changelog") so operators can alert on either pipeline independently.
	childTruncated *service.MetricCounter

	// runtime state populated by Connect.
	curMu     sync.RWMutex
	cur       cursor
	connected atomic.Bool
	page      *pageState
	nextToken string
}

// currentCursor returns a copy of the current cursor under read lock.
func (r *reader) currentCursor() cursor {
	r.curMu.RLock()
	defer r.curMu.RUnlock()
	return r.cur
}

// setCursor replaces the current cursor under write lock.
func (r *reader) setCursor(c cursor) {
	r.curMu.Lock()
	defer r.curMu.Unlock()
	r.cur = c
}

// issuesPage is the subset of /rest/api/3/search/jql response we use.
type issuesPage struct {
	Issues        []json.RawMessage `json:"issues"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
}

// rawIssue holds the fields we need to derive metadata.
type rawIssue struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Self   string `json:"self"`
	Fields struct {
		Project struct {
			Key string `json:"key"`
		} `json:"project"`
		Updated jiraTime `json:"updated"`
	} `json:"fields"`
}

// jiraTime parses Jira's `2026-06-01T10:00:00.000+0000` format.
type jiraTime struct{ time.Time }

func (j *jiraTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	// Jira returns offset without colon ("+0000"); RFC3339 wants "+00:00".
	for _, layout := range []string{"2006-01-02T15:04:05.999Z0700", "2006-01-02T15:04:05.999-07:00", time.RFC3339Nano} {
		if t, err := time.Parse(layout, s); err == nil {
			j.Time = t
			return nil
		}
	}
	return fmt.Errorf("unrecognised jira time format: %q", s)
}

// pageState tracks ack progress for the in-flight page. Buffer/index/maxUpdated
// are mu-guarded. outstandingAcks/pageHasNack are atomic to keep the ack hot
// path lock-free. done is closed by the ack callback when the last ack settles;
// the Read goroutine then advances the cursor before fetching the next page,
// ensuring the cursor write happens-before the next JQL is built.
type pageState struct {
	mu              sync.Mutex
	buffer          []*service.Message
	bufferIdx       int
	outstandingAcks atomic.Int32
	pageHasNack     atomic.Bool
	pageMaxUpdated  time.Time
	done            chan struct{}
}

// nextBufferedMessage returns the next pending message from the page buffer.
// The outstanding-ack counter is pre-loaded in load(), so dispatching does not
// touch it - that keeps the close-once invariant on pageState.done simple.
// ok is false when the buffer is drained.
func (p *pageState) nextBufferedMessage() (msg *service.Message, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bufferIdx >= len(p.buffer) {
		return nil, false
	}
	msg = p.buffer[p.bufferIdx]
	p.bufferIdx++
	return msg, true
}

// isEmpty reports whether the buffer has no remaining messages.
func (p *pageState) isEmpty() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bufferIdx >= len(p.buffer)
}

// load atomically replaces the buffer contents with msgs, records the max
// updated timestamp observed for the page, and arms a fresh done channel.
// outstandingAcks is pre-loaded to len(msgs) so the ack callback only needs
// to decrement. An empty page has its done channel closed immediately so the
// Read loop never blocks waiting for acks that will never fire.
func (p *pageState) load(msgs []*service.Message, maxUpdated time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buffer = msgs
	p.bufferIdx = 0
	p.pageMaxUpdated = maxUpdated
	p.done = make(chan struct{})
	p.outstandingAcks.Store(int32(len(msgs)))
	if len(msgs) == 0 {
		close(p.done)
	}
}

// reset clears all per-page state in preparation for the next page fetch.
// done is cleared so a subsequent waitForPageAcks call before the next load
// is a no-op.
func (p *pageState) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buffer = nil
	p.bufferIdx = 0
	p.pageHasNack.Store(false)
	p.pageMaxUpdated = time.Time{}
	p.done = nil
}

// bufferLen returns the number of messages in the current page buffer.
func (p *pageState) bufferLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buffer)
}

// maxUpdated returns the highest issue.updated timestamp seen in the buffer.
func (p *pageState) maxUpdated() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pageMaxUpdated
}

// currentDone returns the done channel for the current page under the page
// mutex so callers don't race load() / reset() reassigning the field.
func (p *pageState) currentDone() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done
}

// allDispatched reports whether every message in the current page has been
// handed out via nextBufferedMessage. Close uses this to decide whether
// waiting on done can ever succeed — if Read returned with un-dispatched
// messages still in the buffer, their acks will never fire and waiting would
// just stall until ctx expires.
func (p *pageState) allDispatched() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bufferIdx >= len(p.buffer)
}

func newReader(conf *service.ParsedConfig, mgr *service.Resources) (*reader, error) {
	cfg, err := parseInputConfig(conf)
	if err != nil {
		return nil, err
	}
	return &reader{
		cfg:            cfg,
		mgr:            mgr,
		log:            mgr.Logger(),
		page:           &pageState{},
		cacheSetErrors: mgr.Metrics().NewCounter("jira_input_cache_set_errors_total"),
		childTruncated: mgr.Metrics().NewCounter("jira_input_child_truncated_total", "resource"),
	}, nil
}

func (r *reader) Connect(ctx context.Context) error {
	if r.connected.Load() {
		return nil
	}
	client, err := jiraauth.BuildClient(r.mgr, r.cfg.httpCfg, r.cfg.authEmail, r.cfg.authAPIToken, r.cfg.pageSize)
	if err != nil {
		return err
	}
	r.client = client

	// Validate auth via /myself.
	myselfURL, err := url.Parse(r.cfg.httpCfg.BaseURL + "/rest/api/3/myself")
	if err != nil {
		return fmt.Errorf("invalid base_url: %w", err)
	}
	if _, err := r.callAPI(ctx, myselfURL); err != nil {
		return fmt.Errorf("authenticating with jira: %w", err)
	}

	// Load cursor from cache.
	c, err := r.readCursor(ctx)
	if err != nil {
		return fmt.Errorf("reading cursor: %w", err)
	}
	r.setCursor(c)
	r.connected.Store(true)
	r.log.Infof("connected to %s as %s", r.cfg.httpCfg.BaseURL, r.cfg.authEmail)
	return nil
}

// callAPI is a thin wrapper around jirahttp.Client.CallAPI.
func (r *reader) callAPI(ctx context.Context, u *url.URL) ([]byte, error) {
	return r.client.CallAPI(ctx, u)
}

func (r *reader) readCursor(ctx context.Context) (cursor, error) {
	var c cursor
	var inner error
	if err := r.mgr.AccessCache(ctx, r.cfg.cacheName, func(cache service.Cache) {
		raw, gerr := cache.Get(ctx, r.cfg.cacheKey)
		if gerr != nil {
			if errors.Is(gerr, service.ErrKeyNotFound) {
				return
			}
			inner = gerr
			return
		}
		if uerr := json.Unmarshal(raw, &c); uerr != nil {
			inner = fmt.Errorf("decoding cursor JSON: %w", uerr)
		}
	}); err != nil {
		return cursor{}, err
	}
	if inner != nil {
		return cursor{}, inner
	}
	return c, nil
}

func (r *reader) writeCursor(ctx context.Context, c cursor) error {
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return r.mgr.AccessCache(ctx, r.cfg.cacheName, func(cache service.Cache) {
		if serr := cache.Set(ctx, r.cfg.cacheKey, raw, nil); serr != nil {
			r.log.Warnf("failed to write cursor to cache: %v", serr)
			r.cacheSetErrors.Incr(1)
		}
	})
}

// Read fetches a new page when the buffer is empty and returns one buffered
// message per call. The ack callback only signals when the last ack settles;
// cursor advancement runs on the Read goroutine inside waitForPageAcks so the
// cursor write strictly happens-before the next fetchNextPage.
func (r *reader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		if msg, ok := r.page.nextBufferedMessage(); ok {
			// Capture the page's done channel at dispatch time so the ack
			// closure doesn't race with load() reassigning r.page.done on the
			// next fetch (load only happens after the previous done is closed
			// and drained, but capturing here keeps the invariant local).
			done := r.page.currentDone()
			ack := func(_ context.Context, ackErr error) error {
				if ackErr != nil {
					r.page.pageHasNack.Store(true)
				}
				if r.page.outstandingAcks.Add(-1) == 0 {
					// Just signal; cursor write happens on the Read goroutine
					// so it's serialised with the next fetch.
					close(done)
				}
				return nil
			}
			return msg, ack, nil
		}

		// Buffer is drained, but acks for the previous page may still be in
		// flight. Wait for them and drain the cursor on this goroutine so the
		// cursor write completes before we build the next request - otherwise
		// the next JQL would be built against a stale cursor.
		if err := r.waitForPageAcks(ctx); err != nil {
			return nil, nil, err
		}

		if err := r.fetchNextPage(ctx); err != nil {
			return nil, nil, err
		}

		if r.page.isEmpty() {
			// Caught up; sleep before polling again. Do not return
			// ErrNotConnected — that would trigger a reconnect cycle.
			select {
			case <-time.After(r.cfg.pollInterval):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			// loop and try again
		}
	}
}

// waitForPageAcks blocks until the in-flight page's done channel is closed by
// the last-firing ack callback, then runs onPageDrained on the caller's
// goroutine. If no page is loaded (first iteration, or already drained), it
// returns immediately. Context cancellation aborts the wait so a consumer that
// never acks cannot deadlock the input.
func (r *reader) waitForPageAcks(ctx context.Context) error {
	done := r.page.currentDone()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		r.onPageDrained(ctx)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *reader) onPageDrained(ctx context.Context) {
	if r.page.pageHasNack.Load() {
		// don't advance cursor; reset for the refetch on next Read
		r.page.reset()
		return
	}
	maxUpdated := r.page.maxUpdated()
	bufferLen := r.page.bufferLen()
	if !maxUpdated.IsZero() && maxUpdated.After(r.currentCursor().Updated) {
		newCur := cursor{Updated: maxUpdated, Version: cursorSchemaVersion}
		r.setCursor(newCur)
		if err := r.writeCursor(ctx, newCur); err != nil {
			r.log.Warnf("writing cursor: %v", err)
		}
		r.log.Infof("advanced cursor to %s after %d issues", newCur.Updated.Format(time.RFC3339), bufferLen)
	}
	r.page.reset()
}

func (r *reader) fetchNextPage(ctx context.Context) error {
	u, err := r.buildSearchURL()
	if err != nil {
		return err
	}
	body, err := r.callAPI(ctx, u)
	if err != nil {
		return fmt.Errorf("fetching jira page: %w", err)
	}
	var page issuesPage
	if err := json.Unmarshal(body, &page); err != nil {
		return fmt.Errorf("decoding jira page: %w", err)
	}

	msgs := make([]*service.Message, 0, len(page.Issues))
	var maxUpdated time.Time
	for _, raw := range page.Issues {
		var meta rawIssue
		if err := json.Unmarshal(raw, &meta); err != nil {
			return fmt.Errorf("decoding issue: %w", err)
		}
		if meta.Fields.Updated.After(maxUpdated) {
			maxUpdated = meta.Fields.Updated.Time
		}

		switch r.cfg.resource {
		case resourceIssues:
			msgs = append(msgs, r.messageFromIssue(raw, meta))
		case resourceComments:
			children, err := r.fetchComments(ctx, meta)
			if err != nil {
				return err
			}
			msgs = append(msgs, children...)
		case resourceChangelog:
			children, err := r.messagesFromChangelog(raw, meta)
			if err != nil {
				return err
			}
			msgs = append(msgs, children...)
		}
	}

	r.page.load(msgs, maxUpdated)
	r.nextToken = page.NextPageToken
	return nil
}

// messageFromIssue converts a single raw issue payload into an emitted message
// with the canonical issue metadata fields.
func (*reader) messageFromIssue(raw json.RawMessage, meta rawIssue) *service.Message {
	m := service.NewMessage(raw)
	m.MetaSetMut("jira_id", meta.Key)
	m.MetaSetMut("jira_project", meta.Fields.Project.Key)
	m.MetaSetMut("jira_updated", meta.Fields.Updated.UTC().Format(time.RFC3339))
	m.MetaSetMut("jira_event_type", "issue")
	m.MetaSetMut("jira_self", meta.Self)
	return m
}

// fetchComments performs a per-issue child fetch against the comments endpoint
// and returns one message per comment. The issue key is URL-path-escaped so
// keys containing slashes or unicode (which Jira technically allows for some
// projects) round-trip safely.
func (r *reader) fetchComments(ctx context.Context, issue rawIssue) ([]*service.Message, error) {
	u, err := url.Parse(r.cfg.httpCfg.BaseURL + "/rest/api/3/issue/" + url.PathEscape(issue.Key) + "/comment")
	if err != nil {
		return nil, err
	}
	body, err := r.callAPI(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("fetching comments for %s: %w", issue.Key, err)
	}
	var envelope struct {
		Comments   []json.RawMessage `json:"comments"`
		Total      int               `json:"total"`
		MaxResults int               `json:"maxResults"`
		StartAt    int               `json:"startAt"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decoding comments: %w", err)
	}
	if envelope.Total > len(envelope.Comments) {
		r.log.Warnf("comments for issue %s truncated: page returned %d of %d (v1 reads only the first page)", issue.Key, len(envelope.Comments), envelope.Total)
		r.childTruncated.Incr(1, "comments")
	}
	msgs := make([]*service.Message, 0, len(envelope.Comments))
	for _, raw := range envelope.Comments {
		var c struct {
			ID      string   `json:"id"`
			Self    string   `json:"self"`
			Updated jiraTime `json:"updated"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("decoding comment: %w", err)
		}
		m := service.NewMessage(raw)
		m.MetaSetMut("jira_id", c.ID)
		m.MetaSetMut("jira_issue_key", issue.Key)
		m.MetaSetMut("jira_project", issue.Fields.Project.Key)
		m.MetaSetMut("jira_updated", c.Updated.UTC().Format(time.RFC3339))
		m.MetaSetMut("jira_event_type", "comment")
		m.MetaSetMut("jira_self", c.Self)
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// messagesFromChangelog decodes the changelog.histories[] array embedded in the
// issue (the search URL auto-augments expand=changelog when resource=changelog)
// and emits one message per history entry. Each message's body is the raw
// history-entry JSON so consumers can inspect items[]; metadata references the
// parent issue and uses history.created for jira_updated.
func (r *reader) messagesFromChangelog(raw json.RawMessage, meta rawIssue) ([]*service.Message, error) {
	var envelope struct {
		Changelog struct {
			Histories  []json.RawMessage `json:"histories"`
			Total      int               `json:"total"`
			MaxResults int               `json:"maxResults"`
			StartAt    int               `json:"startAt"`
		} `json:"changelog"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("decoding changelog: %w", err)
	}
	if envelope.Changelog.Total > len(envelope.Changelog.Histories) {
		r.log.Warnf("changelog for issue %s truncated: page returned %d of %d (v1 reads only the first page)", meta.Key, len(envelope.Changelog.Histories), envelope.Changelog.Total)
		r.childTruncated.Incr(1, "changelog")
	}
	msgs := make([]*service.Message, 0, len(envelope.Changelog.Histories))
	for _, hraw := range envelope.Changelog.Histories {
		var h struct {
			ID      string   `json:"id"`
			Created jiraTime `json:"created"`
		}
		if err := json.Unmarshal(hraw, &h); err != nil {
			return nil, fmt.Errorf("decoding history entry: %w", err)
		}
		m := service.NewMessage(hraw)
		m.MetaSetMut("jira_id", h.ID)
		m.MetaSetMut("jira_issue_key", meta.Key)
		m.MetaSetMut("jira_project", meta.Fields.Project.Key)
		m.MetaSetMut("jira_updated", h.Created.UTC().Format(time.RFC3339))
		m.MetaSetMut("jira_event_type", "changelog")
		// History entries have no `self` URL of their own; fall back to the
		// parent issue's self so the metadata contract holds for every message.
		m.MetaSetMut("jira_self", meta.Self)
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (r *reader) buildSearchURL() (*url.URL, error) {
	u, err := url.Parse(r.cfg.httpCfg.BaseURL + "/rest/api/3/search/jql")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("jql", r.buildJQL())
	q.Set("fields", strings.Join(r.cfg.fields, ","))
	expand := r.cfg.expand
	if r.cfg.resource == resourceChangelog {
		expand = appendUnique(expand, "changelog")
	}
	if len(expand) > 0 {
		q.Set("expand", strings.Join(expand, ","))
	}
	q.Set("maxResults", strconv.Itoa(r.cfg.pageSize))
	if r.nextToken != "" {
		q.Set("nextPageToken", r.nextToken)
	}
	u.RawQuery = q.Encode()
	return u, nil
}

func (r *reader) buildJQL() string {
	parts := []string{}
	if r.cfg.jql != "" {
		parts = append(parts, "("+r.cfg.jql+")")
	}
	cur := r.currentCursor()
	if !cur.Updated.IsZero() {
		threshold := cur.Updated.Add(-r.cfg.cursorOverlap)
		parts = append(parts, fmt.Sprintf(`updated >= "%s"`, threshold.UTC().Format("2006-01-02 15:04")))
	}
	jql := strings.Join(parts, " AND ")
	if jql != "" {
		jql += " "
	}
	return jql + "ORDER BY updated ASC, key ASC"
}

func appendUnique(s []string, v string) []string {
	if slices.Contains(s, v) {
		return s
	}
	return append(s, v)
}

// closeAckDrainTimeout caps how long Close will wait for in-flight page acks
// to settle before returning. The benthos shutdown path abandons un-acked
// messages once the stream finishes tearing down, so blocking on the full
// shutdown ctx would just stall — the overlap window covers the replay.
const closeAckDrainTimeout = 500 * time.Millisecond

// Close drains any in-flight page acks so the cursor can advance for the last
// page before tearing down, then resets the connected flag so a future Connect
// re-reads the cursor from the cache. The wait is bounded by both the passed
// ctx and a short internal timeout — if neither completes first, the cursor
// stays where it is and the next run will replay (within the overlap window).
// If the Read goroutine exited with un-dispatched messages still in the
// buffer, their acks will never fire, so we skip the wait — those messages
// were never seen by consumers and must not advance the cursor.
func (r *reader) Close(ctx context.Context) error {
	if done := r.page.currentDone(); done != nil && r.page.allDispatched() {
		drainCtx, cancel := context.WithTimeout(ctx, closeAckDrainTimeout)
		defer cancel()
		select {
		case <-done:
			r.onPageDrained(ctx)
		case <-drainCtx.Done():
			// Bail out — cursor stays where it is; next Connect will resume.
		}
	}
	r.connected.Store(false)
	return nil
}

func init() {
	service.MustRegisterInput(
		"jira", newJiraInputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			r, err := newReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, r)
		},
	)
}
