// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package pipelinestatus serves the /status endpoint of a pipeline: the
// connection status of its components, a messages consumed counter with the
// time of the latest one, the backlog its inputs report, and the most recent
// error logs per component. It depends only on the public benthos service
// API, and is fed through the CLI's log and metrics tees.
package pipelinestatus

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/redact"
)

// Path is the HTTP path the endpoint is served under.
const Path = "/status"

// Description is the endpoint description shown by /endpoints.
const Description = "Returns the connection status of the running pipeline, a messages consumed counter with the time of the latest one, the backlog its inputs report, and the most recent error logs per component."

const (
	connStatusOK           = "OK"
	connStatusError        = "ERROR"
	connStatusInitializing = "INITIALIZING"

	// recentErrorsPerComponent is how many ERROR level log entries are kept
	// for each component, keyed by its label and path.
	recentErrorsPerComponent = 10
)

// response is the body served by the endpoint.
type response struct {
	Connection       connection `json:"connection"`
	MessagesConsumed int64      `json:"messages_consumed"`
	// MessagesRemaining is the number of messages the inputs report remain
	// at their sources, the sum of every `input_pending` gauge series the
	// inputs have set, and null when none has.
	MessagesRemaining *int64 `json:"messages_remaining"`
	// LastMessageReceivedAt is the unix time an input last produced a
	// message, null until the first one. It moves at the same event that
	// MessagesConsumed counts: when the input hands the batch over, before
	// anything downstream acknowledges it.
	LastMessageReceivedAt *int64     `json:"last_message_received_at"`
	RecentErrors          []logEntry `json:"recent_errors"`
}

type connection struct {
	Status string            `json:"status"`
	Errors []connectionError `json:"errors"`
}

type connectionError struct {
	Label   string `json:"label,omitempty"`
	Path    string `json:"path"`
	Message string `json:"message"`
}

type logEntry struct {
	Stream    string `json:"stream,omitempty"`
	Label     string `json:"label,omitempty"`
	Path      string `json:"path"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`

	// seq orders entries by when they were recorded, since Timestamp has
	// whole second resolution.
	seq uint64
}

// Endpoint holds the in-memory state behind /status: the running stream
// summary for connection statuses, a messages consumed counter fed by the
// metrics tee, and the most recent ERROR level logs per component fed by the
// log tee.
type Endpoint struct {
	enabled  atomic.Bool
	summary  atomic.Pointer[service.RunningStreamSummary]
	consumed atomic.Int64
	// lastReceivedAt is the unix time of the latest consumed message, zero
	// before the first.
	lastReceivedAt atomic.Int64
	pending        pendingSeries
	recent         *recentErrors
}

// New returns an Endpoint with recording disabled.
func New() *Endpoint {
	return &Endpoint{
		recent: newRecentErrors(recentErrorsPerComponent),
	}
}

// SetEnabled turns recording on. While off, the log handler ignores every
// record and the metrics tee hands out no-op counters, so the only cost of
// the disabled endpoint is the extra no-op call the tee adds to each metric
// operation. It must be called before components are constructed, which the
// CLI does when it parses its flags.
func (e *Endpoint) SetEnabled(enabled bool) {
	e.enabled.Store(enabled)
}

// Enabled reports whether recording is on.
func (e *Endpoint) Enabled() bool {
	return e.enabled.Load()
}

// SetSummary stores the summary of the running stream, which the connection
// part of the response is read from. Until it is set the status is
// INITIALIZING. In streams mode the summary aggregates the connections of
// every stream, and the benthos API attaches no stream ID to a connection, so
// an entry under connection.errors names its component path only. Entries
// under recent_errors do carry their stream.
//
// Reading the summary takes the same locks benthos holds while it swaps a
// stream on a config reload, creates a stream in streams mode, or shuts down,
// so a request to the endpoint can stall for the length of those operations,
// as /ready does today.
func (e *Endpoint) SetSummary(summary *service.RunningStreamSummary) {
	e.summary.Store(summary)
}

// LogHandler returns a slog handler that records ERROR level entries. It must
// receive the logs of every component, which the CLI's tee logger provides.
func (e *Endpoint) LogHandler() slog.Handler {
	return &errorRecorder{enabled: &e.enabled, recent: e.recent}
}

// MetricsTee returns a metrics exporter that sums the consumed metric. It must
// be installed as a tee alongside the configured exporter.
func (e *Endpoint) MetricsTee() service.MetricsExporter {
	return messagesConsumedTee{total: &e.consumed, lastAt: &e.lastReceivedAt, pending: &e.pending, enabled: &e.enabled}
}

// Handler returns the HTTP handler of the endpoint.
func (e *Endpoint) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		body, err := json.Marshal(e.response())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

func (e *Endpoint) response() response {
	res := response{
		Connection: connection{
			Status: connStatusInitializing,
			Errors: []connectionError{},
		},
		MessagesConsumed: e.consumed.Load(),
		RecentErrors:     e.recent.entries(),
	}
	if at := e.lastReceivedAt.Load(); at != 0 {
		res.LastMessageReceivedAt = &at
	}
	if n, ok := e.pending.total(); ok {
		res.MessagesRemaining = &n
	}

	if summary := e.summary.Load(); summary != nil {
		res.Connection = connectionResponse(InactiveConnections(summary))
	}
	return res
}

//------------------------------------------------------------------------------

// ConnectionIssue is a component connection that is not active. Path is the
// dot path of the component in the config.
type ConnectionIssue struct {
	Label string
	Path  string
	Err   error
}

// InactiveConnections returns every component connection of the stream that
// is not currently active, with or without an error. The Kafka status emitter
// reports all of them; the endpoint reports only the ones with an error, see
// connectionResponse.
func InactiveConnections(summary *service.RunningStreamSummary) []ConnectionIssue {
	var issues []ConnectionIssue
	for _, c := range summary.ConnectionStatuses() {
		if c.Active() {
			continue
		}
		issues = append(issues, ConnectionIssue{
			Label: c.Label(),
			Path:  sliceToDotPath(c.Path()),
			Err:   c.Err(),
		})
	}
	return issues
}

// connectionResponse builds the connection part of the response from the
// inactive connections of a started stream. Only connections with an error
// are reported. An inactive connection without one is either pending, not yet
// attempted or mid-attempt, or was closed on purpose, such as an input that
// has produced everything it was asked to, and benthos represents the two the
// same way. Treating them as fine means a pending connection reads as OK
// until its attempt fails, which is bounded by the component's dial timeout,
// whereas treating them as a problem would flag every finished input for the
// rest of the pipeline's life. Error messages are redacted.
func connectionResponse(issues []ConnectionIssue) connection {
	conn := connection{
		Status: connStatusOK,
		Errors: make([]connectionError, 0, len(issues)),
	}
	for _, c := range issues {
		if c.Err == nil {
			continue
		}
		conn.Status = connStatusError
		conn.Errors = append(conn.Errors, connectionError{
			Label:   c.Label,
			Path:    c.Path,
			Message: redact.Error(c.Err).Error(),
		})
	}
	return conn
}

// sliceToDotPath joins path segments with dots, escaping `~` as `~0` and `.`
// as `~1` so the result can be split back into the same segments.
func sliceToDotPath(path []string) string {
	var b strings.Builder
	for i, s := range path {
		s = strings.ReplaceAll(s, "~", "~0")
		s = strings.ReplaceAll(s, ".", "~1")
		b.WriteString(s)
		if i < len(path)-1 {
			b.WriteByte('.')
		}
	}
	return b.String()
}
