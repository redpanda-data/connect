// Copyright 2024 Redpanda Data, Inc.
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

package bluesky

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Jetstream public instances
var defaultJetstreamEndpoints = []string{
	"wss://jetstream1.us-east.bsky.network/subscribe",
	"wss://jetstream2.us-east.bsky.network/subscribe",
	"wss://jetstream1.us-west.bsky.network/subscribe",
	"wss://jetstream2.us-west.bsky.network/subscribe",
}

const (
	defaultJetstreamBufferSize      = 1024
	defaultJetstreamCursorCommitInt = time.Second
	defaultJetstreamPingInterval    = 30 * time.Second
	pingWriteTimeout                = 10 * time.Second
)

type jetstreamMessage struct {
	raw   []byte
	event JetstreamEvent
}

// JetstreamEvent represents a message from the Jetstream firehose
type JetstreamEvent struct {
	Did    string `json:"did"`
	TimeUS int64  `json:"time_us"`
	Kind   string `json:"kind"` // "commit", "identity", "account"

	// For commit events
	Commit *CommitEvent `json:"commit,omitempty"`

	// For identity events
	Identity *IdentityEvent `json:"identity,omitempty"`

	// For account events
	Account *AccountEvent `json:"account,omitempty"`
}

// CommitEvent represents a repository commit (create/update/delete)
type CommitEvent struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"` // "create", "update", "delete"
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	Cid        string          `json:"cid,omitempty"`
}

// IdentityEvent represents a handle/identity change
type IdentityEvent struct {
	Did    string `json:"did"`
	Handle string `json:"handle"`
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
}

// AccountEvent represents an account status change
type AccountEvent struct {
	Active bool   `json:"active"`
	Did    string `json:"did"`
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
	Status string `json:"status,omitempty"` // "takendown", "suspended", "deleted", "deactivated"
}

// jetstreamMetadataDescription returns the metadata documentation block.
func jetstreamMetadataDescription() string {
	return `

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- bluesky_did
- bluesky_kind
- bluesky_time_us
- bluesky_collection (for commit events)
- bluesky_operation (for commit events)
- bluesky_rkey (for commit events)
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).
`
}

func jetstreamInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services", "Social").
		Summary("Connects to the Bluesky Jetstream firehose to consume real-time events from the AT Protocol network.").
		Description(`
This input connects to [Bluesky's Jetstream](https://docs.bsky.app/blog/jetstream) service, which provides a simplified,
JSON-based stream of events from the AT Protocol network. Events include posts, likes, follows, reposts, and more.

Jetstream is a lightweight alternative to the full AT Protocol firehose, making it easier to build applications that
consume real-time Bluesky data without dealing with complex CBOR/CAR binary formats.

### Event Types

The stream includes three types of events:

- **commit**: Repository changes (posts, likes, follows, blocks, etc.)
- **identity**: Handle and identity changes
- **account**: Account status changes (active, suspended, deleted)

### Custom PDS Support

While the default configuration connects to Bluesky's public Jetstream servers, you can point this input to any
AT Protocol PDS (Personal Data Server) that exposes a Jetstream-compatible endpoint. This enables:

- Reading from self-hosted PDS instances
- Connecting to alternative AT Protocol networks
- Testing against local development servers

### Cursor-Based Resumption

The input supports cursor-based resumption via an optional cache resource. When configured, the cursor (Unix
microsecond timestamp) of the last processed event is stored in the cache. On restart, the stream resumes
from that point, ensuring no events are missed.

### High Volume Tuning

For high throughput workloads consider enabling compress, setting a larger buffer_size, and using
cursor_commit_interval to reduce cache write amplification. You can also tune max_in_flight, ping_interval,
and read_timeout for additional control.
`+jetstreamMetadataDescription()).
		Fields(
			service.NewStringField("endpoint").
				Description("The Jetstream WebSocket endpoint URL. Leave empty to use the default public endpoints with automatic failover.").
				Example("wss://jetstream1.us-east.bsky.network/subscribe").
				Example("wss://my-pds.example.com/subscribe").
				Default("").
				Advanced(),
			service.NewStringListField("endpoints").
				Description("A list of Jetstream WebSocket endpoints to try in order. Ignored when `endpoint` is set. Leave empty to use the default public endpoints.").
				Example([]string{"wss://jetstream1.us-east.bsky.network/subscribe", "wss://jetstream2.us-east.bsky.network/subscribe"}).
				Default([]string{}).
				Advanced(),
			service.NewStringListField("collections").
				Description("Filter events by collection NSID (Namespaced Identifier). Supports wildcards like `app.bsky.feed.*`. Leave empty to receive all collections.").
				Example([]string{"app.bsky.feed.post"}).
				Example([]string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"}).
				Example([]string{"app.bsky.feed.*"}).
				Default([]string{}),
			service.NewStringListField("dids").
				Description("Filter events by repository DID (Decentralized Identifier). Maximum 10,000 DIDs. Leave empty to receive events from all users.").
				Example([]string{"did:plc:z72i7hdynmk6r22z27h6tvur"}).
				Default([]string{}),
			service.NewBoolField("compress").
				Description("Enable zstd compression for reduced bandwidth. Recommended for high-volume streams.").
				Default(false).
				Advanced(),
			service.NewIntField("buffer_size").
				Description("Size of the internal message buffer. Higher values can absorb bursts but use more memory.").
				Default(defaultJetstreamBufferSize).
				Advanced(),
			service.NewIntField("read_limit_bytes").
				Description("Maximum size of a WebSocket message in bytes. Set to 0 for no limit.").
				Default(0).
				Advanced(),
			service.NewDurationField("ping_interval").
				Description("Interval for sending WebSocket ping frames to keep connections alive. Set to 0 to disable.").
				Default(defaultJetstreamPingInterval.String()).
				Advanced(),
			service.NewDurationField("read_timeout").
				Description("Read deadline for WebSocket messages. If set to 0 and `ping_interval` is enabled, a default of 2x `ping_interval` is used.").
				Default("0s").
				Advanced(),
			service.NewStringField("cache").
				Description("A cache resource for storing the cursor position. This enables resumption from the last processed event after restarts.").
				Default("").
				Advanced(),
			service.NewStringField("cache_key").
				Description("The key used to store the cursor in the cache.").
				Default("bluesky_jetstream_cursor").
				Advanced(),
			service.NewIntField("cursor_buffer_us").
				Description("When resuming, subtract this many microseconds from the stored cursor to ensure no events are missed due to timing issues. Default is 1 second (1,000,000 microseconds).").
				Default(1000000).
				Advanced(),
			service.NewDurationField("cursor_commit_interval").
				Description("Minimum interval between cursor commits to the cache. Set to 0 to commit every acknowledged message.").
				Default(defaultJetstreamCursorCommitInt.String()).
				Advanced(),
			service.NewInputMaxInFlightField(),
			service.NewAutoRetryNacksToggleField(),
		).
		LintRule(`
			root = if this.collections.or([]).length() > 100 {
				"maximum of 100 collections allowed"
			}
		`).
		LintRule(`
			root = if this.dids.or([]).length() > 10000 {
				"maximum of 10,000 DIDs allowed"
			}
		`).
		LintRule(`
			root = if this.buffer_size.or(1024) <= 0 {
				"buffer_size must be greater than 0"
			}
		`)
}

func init() {
	err := service.RegisterInput(
		"bluesky_jetstream", jetstreamInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newJetstreamReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			input, err := service.AutoRetryNacksToggled(conf, reader)
			if err != nil {
				return nil, err
			}
			return service.InputWithMaxInFlight(reader.maxInFlight, input), nil
		},
	)
	if err != nil {
		panic(err)
	}
}

type jetstreamReader struct {
	log     *service.Logger
	shutSig *shutdown.Signaller
	mgr     *service.Resources

	checkpointer *checkpoint.Capped[int64]

	// Config
	endpoint       string
	endpoints      []string
	collections    []string
	dids           []string
	compress       bool
	bufferSize     int
	readLimitBytes int64
	pingInterval   time.Duration
	readTimeout    time.Duration
	cache          string
	cacheKey       string
	cursorBufferUS int64
	cursorCommit   time.Duration
	maxInFlight    int

	// Connection state
	connMut    sync.Mutex
	conn       *websocket.Conn
	msgChan    chan jetstreamMessage
	zstdReader *zstd.Decoder
	endpointIx int

	cursorMu       sync.Mutex
	cursorPending  int64
	cursorLastSave time.Time
}

func newJetstreamReader(conf *service.ParsedConfig, mgr *service.Resources) (*jetstreamReader, error) {
	r := &jetstreamReader{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
		mgr:     mgr,
	}
	var err error

	if r.endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if r.endpoints, err = conf.FieldStringList("endpoints"); err != nil {
		return nil, err
	}
	if r.collections, err = conf.FieldStringList("collections"); err != nil {
		return nil, err
	}
	if r.dids, err = conf.FieldStringList("dids"); err != nil {
		return nil, err
	}
	if r.compress, err = conf.FieldBool("compress"); err != nil {
		return nil, err
	}
	if r.bufferSize, err = conf.FieldInt("buffer_size"); err != nil {
		return nil, err
	}
	readLimitBytes, err := conf.FieldInt("read_limit_bytes")
	if err != nil {
		return nil, err
	}
	r.readLimitBytes = int64(readLimitBytes)
	if r.pingInterval, err = conf.FieldDuration("ping_interval"); err != nil {
		return nil, err
	}
	if r.readTimeout, err = conf.FieldDuration("read_timeout"); err != nil {
		return nil, err
	}
	if r.cache, err = conf.FieldString("cache"); err != nil {
		return nil, err
	}
	if r.cacheKey, err = conf.FieldString("cache_key"); err != nil {
		return nil, err
	}
	if r.cursorCommit, err = conf.FieldDuration("cursor_commit_interval"); err != nil {
		return nil, err
	}
	if r.maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
		return nil, err
	}

	cursorBuffer, err := conf.FieldInt("cursor_buffer_us")
	if err != nil {
		return nil, err
	}
	r.cursorBufferUS = int64(cursorBuffer)

	// Validate configuration
	if r.bufferSize <= 0 {
		return nil, errors.New("buffer_size must be greater than 0")
	}
	if r.readLimitBytes < 0 {
		return nil, errors.New("read_limit_bytes must be greater than or equal to 0")
	}
	if r.pingInterval < 0 {
		return nil, errors.New("ping_interval must be greater than or equal to 0")
	}
	if r.readTimeout < 0 {
		return nil, errors.New("read_timeout must be greater than or equal to 0")
	}
	if r.cursorBufferUS < 0 {
		return nil, errors.New("cursor_buffer_us must be greater than or equal to 0")
	}
	if r.cursorCommit < 0 {
		return nil, errors.New("cursor_commit_interval must be greater than or equal to 0")
	}
	if len(r.collections) > 100 {
		return nil, errors.New("maximum of 100 collections allowed")
	}
	if len(r.dids) > 10000 {
		return nil, errors.New("maximum of 10,000 DIDs allowed")
	}
	if r.readTimeout == 0 && r.pingInterval > 0 {
		r.readTimeout = r.pingInterval * 2
	}

	checkpointLimit := defaultJetstreamBufferSize
	if r.bufferSize > checkpointLimit {
		checkpointLimit = r.bufferSize
	}
	if r.maxInFlight > checkpointLimit {
		checkpointLimit = r.maxInFlight
	}
	r.checkpointer = checkpoint.NewCapped[int64](int64(checkpointLimit))

	return r, nil
}

func (r *jetstreamReader) endpointCandidates() []string {
	if r.endpoint != "" {
		return []string{r.endpoint}
	}
	if len(r.endpoints) > 0 {
		return r.endpoints
	}
	return defaultJetstreamEndpoints
}

func (r *jetstreamReader) nextEndpoints() []string {
	endpoints := r.endpointCandidates()
	if len(endpoints) <= 1 {
		return endpoints
	}
	start := r.endpointIx % len(endpoints)
	r.endpointIx = (start + 1) % len(endpoints)
	ordered := make([]string, 0, len(endpoints))
	ordered = append(ordered, endpoints[start:]...)
	ordered = append(ordered, endpoints[:start]...)
	return ordered
}

func (r *jetstreamReader) buildURL(endpoint string, cursor int64) (string, error) {
	if endpoint == "" {
		return "", errors.New("endpoint cannot be empty")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	q := u.Query()

	// Add collection filters
	for _, collection := range r.collections {
		q.Add("wantedCollections", collection)
	}

	// Add DID filters
	for _, did := range r.dids {
		q.Add("wantedDids", did)
	}

	// Add compression flag
	if r.compress {
		q.Set("compress", "true")
	}

	// Add cursor for resumption
	if cursor > 0 {
		q.Set("cursor", strconv.FormatInt(cursor, 10))
	}

	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (r *jetstreamReader) getCursor(ctx context.Context) (int64, error) {
	if r.cache == "" {
		return 0, nil
	}

	var cursor int64
	var cacheErr error

	err := r.mgr.AccessCache(ctx, r.cache, func(c service.Cache) {
		var cursorBytes []byte
		if cursorBytes, cacheErr = c.Get(ctx, r.cacheKey); errors.Is(cacheErr, service.ErrKeyNotFound) {
			cacheErr = nil
			return
		}
		if cacheErr != nil {
			return
		}
		cursor, cacheErr = strconv.ParseInt(string(cursorBytes), 10, 64)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to access cache: %w", err)
	}
	if cacheErr != nil {
		return 0, fmt.Errorf("failed to read cursor from cache: %w", cacheErr)
	}

	// Apply buffer to ensure no events are missed
	if cursor > r.cursorBufferUS {
		cursor -= r.cursorBufferUS
	}

	return cursor, nil
}

func (r *jetstreamReader) saveCursor(ctx context.Context, cursor int64) error {
	if r.cache == "" {
		return nil
	}

	var setErr error
	if err := r.mgr.AccessCache(ctx, r.cache, func(c service.Cache) {
		setErr = c.Set(ctx, r.cacheKey, []byte(strconv.FormatInt(cursor, 10)), nil)
	}); err != nil {
		return err
	}
	return setErr
}

func (r *jetstreamReader) commitCursor(ctx context.Context, cursor int64) error {
	if r.cache == "" || cursor <= 0 {
		return nil
	}

	r.cursorMu.Lock()
	if cursor > r.cursorPending {
		r.cursorPending = cursor
	}
	cursorToCommit := r.cursorPending
	commitInterval := r.cursorCommit
	lastCommit := r.cursorLastSave
	shouldCommit := commitInterval <= 0 || lastCommit.IsZero() || time.Since(lastCommit) >= commitInterval
	r.cursorMu.Unlock()

	if !shouldCommit || cursorToCommit <= 0 {
		return nil
	}

	if err := r.saveCursor(ctx, cursorToCommit); err != nil {
		return err
	}

	r.cursorMu.Lock()
	r.cursorLastSave = time.Now()
	r.cursorMu.Unlock()
	return nil
}

func (r *jetstreamReader) flushCursor(ctx context.Context) error {
	if r.cache == "" {
		return nil
	}
	r.cursorMu.Lock()
	cursor := r.cursorPending
	r.cursorMu.Unlock()
	if cursor <= 0 {
		return nil
	}
	return r.saveCursor(ctx, cursor)
}

func (r *jetstreamReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	if r.msgChan != nil {
		r.connMut.Unlock()
		return nil
	}
	endpoints := r.nextEndpoints()
	r.connMut.Unlock()

	// Get cursor for resumption
	cursor, err := r.getCursor(ctx)
	if err != nil {
		r.log.Warnf("Failed to get cursor from cache, starting from live: %v", err)
		cursor = 0
	}

	if cursor > 0 {
		r.log.Infof("Resuming from cursor: %d (approximately %s ago)",
			cursor, time.Since(time.UnixMicro(cursor)).Round(time.Second))
	}

	// Connect with custom headers for compression
	dialer := websocket.DefaultDialer
	headers := make(map[string][]string)
	if r.compress {
		headers["Socket-Encoding"] = []string{"zstd"}
	}

	var (
		conn  *websocket.Conn
		wsURL string
	)
	var lastErr error
	for _, endpoint := range endpoints {
		wsURL, err = r.buildURL(endpoint, cursor)
		if err != nil {
			lastErr = err
			r.log.Warnf("Invalid Jetstream endpoint %q: %v", endpoint, err)
			continue
		}

		r.log.Debugf("Connecting to Jetstream: %s", wsURL)
		var resp *http.Response
		conn, resp, err = dialer.DialContext(ctx, wsURL, headers)
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil {
			lastErr = err
			r.log.Debugf("Failed to connect to Jetstream endpoint %q: %v", endpoint, err)
			conn = nil
			continue
		}
		break
	}
	if conn == nil {
		if lastErr == nil {
			lastErr = errors.New("no endpoints available")
		}
		return fmt.Errorf("failed to connect to Jetstream: %w", lastErr)
	}

	if r.readLimitBytes > 0 {
		conn.SetReadLimit(r.readLimitBytes)
	}
	if r.readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(r.readTimeout))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(r.readTimeout))
		})
	}

	// Initialize zstd decoder if compression is enabled
	if r.compress {
		r.zstdReader, err = zstd.NewReader(nil)
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to create zstd decoder: %w", err)
		}
	}

	msgChan := make(chan jetstreamMessage, r.bufferSize)
	connClosed := make(chan struct{})

	r.connMut.Lock()
	if r.msgChan != nil {
		r.connMut.Unlock()
		close(connClosed)
		_ = conn.Close()
		if r.zstdReader != nil {
			r.zstdReader.Close()
			r.zstdReader = nil
		}
		return nil
	}
	r.conn = conn
	r.msgChan = msgChan
	r.connMut.Unlock()

	if r.pingInterval > 0 {
		go func() {
			ticker := time.NewTicker(r.pingInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_ = conn.SetWriteDeadline(time.Now().Add(pingWriteTimeout))
					if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
						if !r.shutSig.IsSoftStopSignalled() {
							r.log.Warnf("WebSocket ping error: %v", err)
						}
						_ = conn.Close()
						return
					}
				case <-connClosed:
					return
				case <-r.shutSig.SoftStopChan():
					return
				}
			}
		}()
	}

	go func() {
		defer func() {
			close(connClosed)
			_ = conn.Close()
			if r.zstdReader != nil {
				r.zstdReader.Close()
				r.zstdReader = nil
			}
			close(msgChan)
			r.connMut.Lock()
			r.conn = nil
			r.connMut.Unlock()
			if r.shutSig.IsSoftStopSignalled() {
				r.shutSig.TriggerHasStopped()
			}
		}()

		for {
			if r.shutSig.IsSoftStopSignalled() {
				return
			}

			if r.readTimeout > 0 {
				_ = conn.SetReadDeadline(time.Now().Add(r.readTimeout))
			}

			_, data, err := conn.ReadMessage()
			if err != nil {
				if !r.shutSig.IsSoftStopSignalled() {
					r.log.Errorf("WebSocket read error: %v", err)
				}
				return
			}

			// Decompress if needed
			if r.compress && r.zstdReader != nil {
				decompressed, err := r.decompressMessage(data)
				if err != nil {
					r.log.Errorf("Failed to decompress message: %v", err)
					continue
				}
				data = decompressed
			}

			var event JetstreamEvent
			if err := json.Unmarshal(data, &event); err != nil {
				r.log.Errorf("Failed to parse Jetstream event: %v", err)
				continue
			}

			select {
			case msgChan <- jetstreamMessage{raw: data, event: event}:
			case <-r.shutSig.SoftStopChan():
				return
			}
		}
	}()

	r.log.Infof("Connected to Bluesky Jetstream: %s", wsURL)
	return nil
}

func (r *jetstreamReader) decompressMessage(data []byte) ([]byte, error) {
	if r.zstdReader == nil {
		return nil, errors.New("zstd reader not initialized")
	}

	err := r.zstdReader.Reset(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r.zstdReader)
}

func (r *jetstreamReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.connMut.Lock()
	msgChan := r.msgChan
	r.connMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var msg jetstreamMessage
	select {
	case m, open := <-msgChan:
		if !open {
			r.connMut.Lock()
			if r.msgChan == msgChan {
				r.msgChan = nil
			}
			r.connMut.Unlock()
			return nil, nil, service.ErrNotConnected
		}
		msg = m
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	// Track this event's cursor for checkpointing
	release, err := r.checkpointer.Track(ctx, msg.event.TimeUS, 1)
	if err != nil {
		return nil, nil, err
	}

	out := service.NewMessage(msg.raw)

	// Add metadata for downstream processing
	out.MetaSet("bluesky_did", msg.event.Did)
	out.MetaSet("bluesky_kind", msg.event.Kind)
	out.MetaSet("bluesky_time_us", strconv.FormatInt(msg.event.TimeUS, 10))

	if msg.event.Commit != nil {
		out.MetaSet("bluesky_collection", msg.event.Commit.Collection)
		out.MetaSet("bluesky_operation", msg.event.Commit.Operation)
		out.MetaSet("bluesky_rkey", msg.event.Commit.Rkey)
	}

	return out, func(ctx context.Context, err error) error {
		highestCursor := release()
		if highestCursor == nil {
			return nil
		}
		return r.commitCursor(ctx, *highestCursor)
	}, nil
}

func (r *jetstreamReader) Close(ctx context.Context) error {
	r.shutSig.TriggerSoftStop()

	r.connMut.Lock()
	conn := r.conn
	if conn == nil {
		r.shutSig.TriggerHasStopped()
	}
	r.connMut.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	select {
	case <-r.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}

	return r.flushCursor(ctx)
}
