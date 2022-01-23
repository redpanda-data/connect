package reader

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	bredis "github.com/Jeffail/benthos/v3/internal/impl/redis"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisStreamsConfig contains configuration fields for the RedisStreams input
// type.
type RedisStreamsConfig struct {
	bredis.Config   `json:",inline" yaml:",inline"`
	BodyKey         string   `json:"body_key" yaml:"body_key"`
	Streams         []string `json:"streams" yaml:"streams"`
	CreateStreams   bool     `json:"create_streams" yaml:"create_streams"`
	ConsumerGroup   string   `json:"consumer_group" yaml:"consumer_group"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	Limit           int64    `json:"limit" yaml:"limit"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
	CommitPeriod    string   `json:"commit_period" yaml:"commit_period"`
	Timeout         string   `json:"timeout" yaml:"timeout"`
}

// NewRedisStreamsConfig creates a new RedisStreamsConfig with default values.
func NewRedisStreamsConfig() RedisStreamsConfig {
	return RedisStreamsConfig{
		Config:          bredis.NewConfig(),
		BodyKey:         "body",
		Streams:         []string{"benthos_stream"},
		CreateStreams:   true,
		ConsumerGroup:   "benthos_group",
		ClientID:        "benthos_consumer",
		Limit:           10,
		StartFromOldest: true,
		CommitPeriod:    "1s",
		Timeout:         "1s",
	}
}

//------------------------------------------------------------------------------

type pendingRedisStreamMsg struct {
	payload types.Message
	stream  string
	id      string
}

// RedisStreams is an input type that reads Redis Streams messages.
type RedisStreams struct {
	client         redis.UniversalClient
	cMut           sync.Mutex
	pendingMsgs    []pendingRedisStreamMsg
	pendingMsgsMut sync.Mutex

	timeout      time.Duration
	commitPeriod time.Duration

	conf RedisStreamsConfig

	backlogs map[string]string

	aMut    sync.Mutex
	ackSend map[string][]string // Acks that can be sent

	deprecatedAckFns []AsyncAckFn

	stats metrics.Type
	log   log.Modular

	closeChan  chan struct{}
	closedChan chan struct{}
	closeOnce  sync.Once
}

// NewRedisStreams creates a new RedisStreams input type.
func NewRedisStreams(
	conf RedisStreamsConfig, log log.Modular, stats metrics.Type,
) (*RedisStreams, error) {
	r := &RedisStreams{
		conf:       conf,
		stats:      stats,
		log:        log,
		backlogs:   make(map[string]string, len(conf.Streams)),
		ackSend:    make(map[string][]string, len(conf.Streams)),
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	for _, str := range conf.Streams {
		r.backlogs[str] = "0"
	}

	if _, err := r.conf.Config.Client(); err != nil {
		return nil, err
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if r.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if tout := conf.CommitPeriod; len(tout) > 0 {
		var err error
		if r.commitPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse commit period string: %v", err)
		}
	}

	go r.loop()
	return r, nil
}

//------------------------------------------------------------------------------

func (r *RedisStreams) loop() {
	defer func() {
		var client redis.UniversalClient
		r.cMut.Lock()
		client = r.client
		r.client = nil
		r.cMut.Unlock()
		if client != nil {
			client.Close()
		}
		close(r.closedChan)
	}()
	commitTimer := time.NewTicker(r.commitPeriod)

	closed := false
	for !closed {
		select {
		case <-commitTimer.C:
		case <-r.closeChan:
			closed = true
		}
		r.sendAcks()
	}
}

func (r *RedisStreams) addAsyncAcks(stream string, ids ...string) {
	r.aMut.Lock()
	if acks, exists := r.ackSend[stream]; exists {
		acks = append(acks, ids...)
		r.ackSend[stream] = acks
	} else {
		r.ackSend[stream] = ids
	}
	r.aMut.Unlock()
}

func (r *RedisStreams) sendAcks() {
	var client redis.UniversalClient
	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return
	}

	r.aMut.Lock()
	ackSend := r.ackSend
	r.ackSend = map[string][]string{}
	r.aMut.Unlock()

	for str, ids := range ackSend {
		if len(ids) == 0 {
			continue
		}
		if err := r.client.XAck(str, r.conf.ConsumerGroup, ids...).Err(); err != nil {
			r.log.Errorf("Failed to ack stream %v: %v\n", str, err)
		}
	}
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a Redis server.
func (r *RedisStreams) Connect() error {
	return r.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to a Redis server.
func (r *RedisStreams) ConnectWithContext(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err := client.Ping().Result(); err != nil {
		return err
	}

	for _, s := range r.conf.Streams {
		offset := "$"
		if r.conf.StartFromOldest {
			offset = "0"
		}
		var err error
		if r.conf.CreateStreams {
			err = client.XGroupCreateMkStream(s, r.conf.ConsumerGroup, offset).Err()
		} else {
			err = client.XGroupCreate(s, r.conf.ConsumerGroup, offset).Err()
		}
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create group %v for stream %v: %v", r.conf.ConsumerGroup, s, err)
		}
	}

	r.log.Infof("Receiving messages from Redis streams: %v\n", r.conf.Streams)

	r.client = client
	return nil
}

func (r *RedisStreams) read() (pendingRedisStreamMsg, error) {
	var client redis.UniversalClient
	var msg pendingRedisStreamMsg

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return msg, types.ErrNotConnected
	}

	r.pendingMsgsMut.Lock()
	defer r.pendingMsgsMut.Unlock()
	if len(r.pendingMsgs) > 0 {
		msg = r.pendingMsgs[0]
		r.pendingMsgs = r.pendingMsgs[1:]
		return msg, nil
	}

	strs := make([]string, len(r.conf.Streams)*2)
	for i, str := range r.conf.Streams {
		strs[i] = str
		if bl := r.backlogs[str]; bl != "" {
			strs[len(r.conf.Streams)+i] = bl
		} else {
			strs[len(r.conf.Streams)+i] = ">"
		}
	}

	res, err := client.XReadGroup(&redis.XReadGroupArgs{
		Block:    r.timeout,
		Consumer: r.conf.ClientID,
		Group:    r.conf.ConsumerGroup,
		Streams:  strs,
		Count:    r.conf.Limit,
	}).Result()

	if err != nil && err != redis.Nil {
		if strings.Contains(err.Error(), "i/o timeout") {
			return msg, types.ErrTimeout
		}
		r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return msg, types.ErrNotConnected
	}

	pendingMsgs := []pendingRedisStreamMsg{}
	for _, strRes := range res {
		if _, exists := r.backlogs[strRes.Stream]; exists {
			if len(strRes.Messages) > 0 {
				r.backlogs[strRes.Stream] = strRes.Messages[len(strRes.Messages)-1].ID
			} else {
				delete(r.backlogs, strRes.Stream)
			}
		}
		for _, xmsg := range strRes.Messages {
			body, exists := xmsg.Values[r.conf.BodyKey]
			if !exists {
				continue
			}
			delete(xmsg.Values, r.conf.BodyKey)

			var bodyBytes []byte
			switch t := body.(type) {
			case string:
				bodyBytes = []byte(t)
			case []byte:
				bodyBytes = t
			}
			if bodyBytes == nil {
				continue
			}

			part := message.NewPart(bodyBytes)
			part.Metadata().Set("redis_stream", xmsg.ID)
			for k, v := range xmsg.Values {
				part.Metadata().Set(k, fmt.Sprintf("%v", v))
			}

			nextMsg := pendingRedisStreamMsg{
				payload: message.New(nil),
				stream:  strRes.Stream,
				id:      xmsg.ID,
			}
			nextMsg.payload.Append(part)
			if msg.payload == nil {
				msg = nextMsg
			} else {
				pendingMsgs = append(pendingMsgs, nextMsg)
			}
		}
	}

	r.pendingMsgs = pendingMsgs
	if msg.payload == nil {
		return msg, types.ErrTimeout
	}
	return msg, nil
}

// ReadWithContext attempts to pop a message from a Redis list.
func (r *RedisStreams) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, err := r.read()
	if err != nil {
		if err == types.ErrTimeout {
			// Allow for one more attempt in case we asked for backlog.
			select {
			case <-ctx.Done():
			default:
				msg, err = r.read()
			}
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return msg.payload, func(rctx context.Context, res types.Response) error {
		if res.Error() != nil {
			r.pendingMsgsMut.Lock()
			r.pendingMsgs = append(r.pendingMsgs, msg)
			r.pendingMsgsMut.Unlock()
		} else {
			r.addAsyncAcks(msg.stream, msg.id)
		}
		return nil
	}, nil
}

// Read attempts to pop a message from a Redis list.
func (r *RedisStreams) Read() (types.Message, error) {
	msg, ackFn, err := r.ReadWithContext(context.Background())
	if err != nil {
		return nil, err
	}
	r.deprecatedAckFns = append(r.deprecatedAckFns, ackFn)
	return msg, nil
}

// Acknowledge is a noop since Redis Lists do not support acknowledgements.
func (r *RedisStreams) Acknowledge(err error) error {
	res := response.NewError(err)
	for _, p := range r.deprecatedAckFns {
		_ = p(context.Background(), res)
	}
	r.deprecatedAckFns = nil
	return nil
}

// disconnect safely closes a connection to an RedisStreams server.
func (r *RedisStreams) disconnect() error {
	r.sendAcks()

	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

// CloseAsync shuts down the RedisStreams input and stops processing requests.
func (r *RedisStreams) CloseAsync() {
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
}

// WaitForClose blocks until the RedisStreams input has closed down.
func (r *RedisStreams) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
