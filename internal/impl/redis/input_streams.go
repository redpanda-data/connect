package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	siFieldBodyKey         = "body_key"
	siFieldStreams         = "streams"
	siFieldLimit           = "limit"
	siFieldClientID        = "client_id"
	siFieldConsumerGroup   = "consumer_group"
	siFieldCreateStreams   = "create_streams"
	siFieldStartFromOldest = "start_from_oldest"
	siFieldCommitPeriod    = "commit_period"
	siFieldTimeout         = "timeout"
)

func redisStreamsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Pulls messages from Redis (v5.0+) streams with the XREADGROUP command. The `+"`client_id`"+` should be unique for each consumer of a group.`).
		Description(`Redis stream entries are key/value pairs, as such it is necessary to specify the key that contains the body of the message. All other keys/value pairs are saved as metadata fields.`).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewStringField(siFieldBodyKey).
				Description("The field key to extract the raw message from. All other keys will be stored in the message as metadata.").
				Default("body"),
			service.NewStringListField(siFieldStreams).
				Description("A list of streams to consume from."),
			service.NewAutoRetryNacksToggleField(),
			service.NewIntField(siFieldLimit).
				Description("The maximum number of messages to consume from a single request.").
				Default(10),
			service.NewStringField(siFieldClientID).
				Description("An identifier for the client connection.").
				Default(""),
			service.NewStringField(siFieldConsumerGroup).
				Description("An identifier for the consumer group of the stream.").
				Default(""),
			service.NewBoolField(siFieldCreateStreams).
				Description("Create subscribed streams if they do not exist (MKSTREAM option).").
				Advanced().
				Default(true),
			service.NewBoolField(siFieldStartFromOldest).
				Description("If an offset is not found for a stream, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset.").
				Advanced().
				Default(true),
			service.NewDurationField(siFieldCommitPeriod).
				Description("The period of time between each commit of the current offset. Offsets are always committed during shutdown.").
				Advanced().
				Default("1s"),
			service.NewDurationField(siFieldTimeout).
				Description("The length of time to poll for new messages before reattempting.").
				Advanced().
				Default("1s"),
		)
}

func init() {
	err := service.RegisterBatchInput(
		"redis_streams", redisStreamsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newRedisStreamsReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, r)
		})
	if err != nil {
		panic(err)
	}
}

type pendingRedisStreamMsg struct {
	payload service.MessageBatch
	stream  string
	id      string
}

type redisStreamsReader struct {
	clientCtor func() (redis.UniversalClient, error)
	client     redis.UniversalClient
	cMut       sync.Mutex

	pendingMsgs    []pendingRedisStreamMsg
	pendingMsgsMut sync.Mutex

	bodyKey         string
	streams         []string
	createStreams   bool
	consumerGroup   string
	clientID        string
	limit           int64
	startFromOldest bool
	commitPeriod    time.Duration
	timeout         time.Duration

	backlogs map[string]string

	aMut    sync.Mutex
	ackSend map[string][]string // Acks that can be sent

	log         *service.Logger
	connBackoff backoff.BackOff

	closeChan  chan struct{}
	closedChan chan struct{}
	closeOnce  sync.Once
}

func newRedisStreamsReader(conf *service.ParsedConfig, mgr *service.Resources) (r *redisStreamsReader, err error) {
	connBoff := backoff.NewExponentialBackOff()
	connBoff.InitialInterval = time.Millisecond * 100
	connBoff.MaxInterval = time.Second
	connBoff.MaxElapsedTime = 0

	r = &redisStreamsReader{
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
		log:         mgr.Logger(),
		connBackoff: connBoff,
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}
	if _, err = getClient(conf); err != nil {
		return
	}

	if r.bodyKey, err = conf.FieldString(siFieldBodyKey); err != nil {
		return
	}
	if r.streams, err = conf.FieldStringList(siFieldStreams); err != nil {
		return
	}
	if r.createStreams, err = conf.FieldBool(siFieldCreateStreams); err != nil {
		return
	}
	if r.consumerGroup, err = conf.FieldString(siFieldConsumerGroup); err != nil {
		return
	}
	if r.clientID, err = conf.FieldString(siFieldClientID); err != nil {
		return
	}
	var tmpLimit int
	if tmpLimit, err = conf.FieldInt(siFieldLimit); err != nil {
		return
	}
	r.limit = int64(tmpLimit)
	if r.startFromOldest, err = conf.FieldBool(siFieldStartFromOldest); err != nil {
		return
	}
	if r.commitPeriod, err = conf.FieldDuration(siFieldCommitPeriod); err != nil {
		return
	}
	if r.timeout, err = conf.FieldDuration(siFieldTimeout); err != nil {
		return
	}

	r.ackSend = make(map[string][]string, len(r.streams))
	r.backlogs = make(map[string]string, len(r.streams))
	for _, str := range r.streams {
		r.backlogs[str] = "0"
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
		if err := client.XAck(ctx, str, r.consumerGroup, ids...).Err(); err != nil {
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

	client, err := r.clientCtor()
	if err != nil {
		return err
	}

	if _, err := client.Ping(ctx).Result(); err != nil {
		return err
	}

	for _, s := range r.streams {
		offset := "$"
		if r.startFromOldest {
			offset = "0"
		}
		var err error
		if r.createStreams {
			err = client.XGroupCreateMkStream(ctx, s, r.consumerGroup, offset).Err()
		} else {
			err = client.XGroupCreate(ctx, s, r.consumerGroup, offset).Err()
		}
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create group %v for stream %v: %v", r.consumerGroup, s, err)
		}
	}
	r.client = client
	return nil
}

func (r *redisStreamsReader) read(ctx context.Context) (pendingRedisStreamMsg, error) {
	var msg pendingRedisStreamMsg

	r.cMut.Lock()
	client := r.client
	r.cMut.Unlock()

	if client == nil {
		return msg, service.ErrNotConnected
	}

	r.pendingMsgsMut.Lock()
	defer r.pendingMsgsMut.Unlock()
	if len(r.pendingMsgs) > 0 {
		msg = r.pendingMsgs[0]
		r.pendingMsgs = r.pendingMsgs[1:]
		return msg, nil
	}

	strs := make([]string, len(r.streams)*2)
	for i, str := range r.streams {
		strs[i] = str
		if bl := r.backlogs[str]; bl != "" {
			strs[len(r.streams)+i] = bl
		} else {
			strs[len(r.streams)+i] = ">"
		}
	}

	res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Block:    r.timeout,
		Consumer: r.clientID,
		Group:    r.consumerGroup,
		Streams:  strs,
		Count:    r.limit,
	}).Result()

	if err != nil && err != redis.Nil {
		if strings.Contains(err.Error(), "i/o timeout") {
			return msg, component.ErrTimeout
		}
		_ = r.disconnect(ctx)
		r.log.Errorf("Error from redis: %v\n", err)

		select {
		case <-time.After(r.connBackoff.NextBackOff()):
		case <-ctx.Done():
		}
		return msg, service.ErrNotConnected
	}
	r.connBackoff.Reset()

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
			body, exists := xmsg.Values[r.bodyKey]
			if !exists {
				continue
			}
			delete(xmsg.Values, r.bodyKey)

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

			part := service.NewMessage(bodyBytes)
			part.MetaSetMut("redis_stream", xmsg.ID)
			for k, v := range xmsg.Values {
				part.MetaSetMut(k, v)
			}

			nextMsg := pendingRedisStreamMsg{
				payload: service.MessageBatch{},
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

func (r *redisStreamsReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
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
