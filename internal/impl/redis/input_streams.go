package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newRedisStreamsInput), docs.ComponentSpec{
		Name: "redis_streams",
		Summary: `
Pulls messages from Redis (v5.0+) streams with the XREADGROUP command. The
` + "`client_id`" + ` should be unique for each consumer of a group.`,
		Description: `
Redis stream entries are key/value pairs, as such it is necessary to specify the
key that contains the body of the message. All other keys/value pairs are saved
as metadata fields.`,
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString("body_key", "The field key to extract the raw message from. All other keys will be stored in the message as metadata."),
			docs.FieldString("streams", "A list of streams to consume from.").Array(),
			docs.FieldInt("limit", "The maximum number of messages to consume from a single request."),
			docs.FieldString("client_id", "An identifier for the client connection."),
			docs.FieldString("consumer_group", "An identifier for the consumer group of the stream."),
			docs.FieldBool("create_streams", "Create subscribed streams if they do not exist (MKSTREAM option).").Advanced(),
			docs.FieldBool("start_from_oldest", "If an offset is not found for a stream, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset.").Advanced(),
			docs.FieldString("commit_period", "The period of time between each commit of the current offset. Offsets are always committed during shutdown.").Advanced(),
			docs.FieldString("timeout", "The length of time to poll for new messages before reattempting.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewRedisStreamsConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisStreamsInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	var c input.Async
	var err error
	if c, err = newRedisStreamsReader(conf.RedisStreams, mgr); err != nil {
		return nil, err
	}
	c = input.NewAsyncPreserver(c)
	return input.NewAsyncReader("redis_streams", c, mgr)
}

type pendingRedisStreamMsg struct {
	payload message.Batch
	stream  string
	id      string
}

type redisStreamsReader struct {
	client         redis.UniversalClient
	cMut           sync.Mutex
	pendingMsgs    []pendingRedisStreamMsg
	pendingMsgsMut sync.Mutex

	timeout      time.Duration
	commitPeriod time.Duration

	conf input.RedisStreamsConfig

	backlogs map[string]string

	aMut    sync.Mutex
	ackSend map[string][]string // Acks that can be sent

	mgr bundle.NewManagement
	log log.Modular

	closeChan  chan struct{}
	closedChan chan struct{}
	closeOnce  sync.Once
}

func newRedisStreamsReader(conf input.RedisStreamsConfig, mgr bundle.NewManagement) (*redisStreamsReader, error) {
	r := &redisStreamsReader{
		conf:       conf,
		log:        mgr.Logger(),
		mgr:        mgr,
		backlogs:   make(map[string]string, len(conf.Streams)),
		ackSend:    make(map[string][]string, len(conf.Streams)),
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	for _, str := range conf.Streams {
		r.backlogs[str] = "0"
	}

	if _, err := clientFromConfig(mgr.FS(), r.conf.Config); err != nil {
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

func (r *redisStreamsReader) loop() {
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

	ctx := context.Background()

	closed := false
	for !closed {
		select {
		case <-commitTimer.C:
		case <-r.closeChan:
			closed = true
		}
		r.sendAcks(ctx)
	}
}

func (r *redisStreamsReader) addAsyncAcks(stream string, ids ...string) {
	r.aMut.Lock()
	if acks, exists := r.ackSend[stream]; exists {
		acks = append(acks, ids...)
		r.ackSend[stream] = acks
	} else {
		r.ackSend[stream] = ids
	}
	r.aMut.Unlock()
}

func (r *redisStreamsReader) sendAcks(ctx context.Context) {
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
		if err := r.client.XAck(ctx, str, r.conf.ConsumerGroup, ids...).Err(); err != nil {
			r.log.Errorf("Failed to ack stream %v: %v\n", str, err)
		}
	}
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a Redis server.
func (r *redisStreamsReader) Connect(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := clientFromConfig(r.mgr.FS(), r.conf.Config)
	if err != nil {
		return err
	}
	if _, err := client.Ping(ctx).Result(); err != nil {
		return err
	}

	for _, s := range r.conf.Streams {
		offset := "$"
		if r.conf.StartFromOldest {
			offset = "0"
		}
		var err error
		if r.conf.CreateStreams {
			err = client.XGroupCreateMkStream(ctx, s, r.conf.ConsumerGroup, offset).Err()
		} else {
			err = client.XGroupCreate(ctx, s, r.conf.ConsumerGroup, offset).Err()
		}
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create group %v for stream %v: %v", r.conf.ConsumerGroup, s, err)
		}
	}

	r.log.Infof("Receiving messages from Redis streams: %v\n", r.conf.Streams)

	r.client = client
	return nil
}

func (r *redisStreamsReader) read(ctx context.Context) (pendingRedisStreamMsg, error) {
	var client redis.UniversalClient
	var msg pendingRedisStreamMsg

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return msg, component.ErrNotConnected
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

	res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Block:    r.timeout,
		Consumer: r.conf.ClientID,
		Group:    r.conf.ConsumerGroup,
		Streams:  strs,
		Count:    r.conf.Limit,
	}).Result()

	if err != nil && err != redis.Nil {
		if strings.Contains(err.Error(), "i/o timeout") {
			return msg, component.ErrTimeout
		}
		_ = r.disconnect(ctx)
		r.log.Errorf("Error from redis: %v\n", err)
		return msg, component.ErrNotConnected
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
			part.MetaSetMut("redis_stream", xmsg.ID)
			for k, v := range xmsg.Values {
				part.MetaSetMut(k, v)
			}

			nextMsg := pendingRedisStreamMsg{
				payload: message.QuickBatch(nil),
				stream:  strRes.Stream,
				id:      xmsg.ID,
			}
			nextMsg.payload = append(nextMsg.payload, part)
			if msg.payload == nil {
				msg = nextMsg
			} else {
				pendingMsgs = append(pendingMsgs, nextMsg)
			}
		}
	}

	r.pendingMsgs = pendingMsgs
	if msg.payload == nil {
		return msg, component.ErrTimeout
	}
	return msg, nil
}

func (r *redisStreamsReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	msg, err := r.read(ctx)
	if err != nil {
		if errors.Is(err, component.ErrTimeout) {
			// Allow for one more attempt in case we asked for backlog.
			select {
			case <-ctx.Done():
			default:
				msg, err = r.read(ctx)
			}
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return msg.payload, func(rctx context.Context, res error) error {
		if res != nil {
			r.pendingMsgsMut.Lock()
			r.pendingMsgs = append(r.pendingMsgs, msg)
			r.pendingMsgsMut.Unlock()
		} else {
			r.addAsyncAcks(msg.stream, msg.id)
		}
		return nil
	}, nil
}

func (r *redisStreamsReader) disconnect(ctx context.Context) error {
	r.sendAcks(ctx)

	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

func (r *redisStreamsReader) Close(ctx context.Context) (err error) {
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
	select {
	case <-r.closedChan:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}
