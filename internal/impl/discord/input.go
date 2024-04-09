package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/bwmarrin/discordgo"

	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

func inputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services", "Social").
		Summary("Consumes messages posted in a Discord channel.").
		Description(`This input works by authenticating as a bot using token based authentication. The ID of the newest message consumed and acked is stored in a cache in order to perform a backfill of unread messages each time the input is initialised. Ideally this cache should be persisted across restarts.`).
		Fields(
			service.NewStringField("channel_id").
				Description("A discord channel ID to consume messages from."),
			service.NewStringField("bot_token").
				Description("A bot token used for authentication."),
			service.NewStringField("cache").
				Description("A cache resource to use for performing unread message backfills, the ID of the last message received will be stored in this cache and used for subsequent requests."),
			service.NewStringField("cache_key").
				Description("The key identifier used when storing the ID of the last message received.").
				Default("last_message_id").
				Advanced(),
			service.NewAutoRetryNacksToggleField(),

			// Deprecated
			service.NewDurationField("poll_period").
				Description("The length of time (as a duration string) to wait between each poll for backlogged messages. This field can be set empty, in which case requests are made at the limit set by the rate limit. This field also supports cron expressions.").
				Default("1m").
				Deprecated(),
			service.NewIntField("limit").
				Description("The maximum number of messages to receive in a single request.").
				Default(100).
				Deprecated(),
			service.NewStringField("rate_limit").
				Description("").
				Default("An optional rate limit resource to restrict API requests with.").
				Deprecated(),
		)
}

func init() {
	err := service.RegisterInput(
		"discord", inputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, reader)
		},
	)
	if err != nil {
		panic(err)
	}
}

type reader struct {
	log     *service.Logger
	shutSig *shutdown.Signaller
	mgr     *service.Resources

	checkpointer *checkpoint.Capped[string]

	// Config
	channelID string
	botToken  string
	cache     string
	cacheKey  string

	connMut sync.Mutex
	msgChan chan *discordgo.Message
}

func newReader(conf *service.ParsedConfig, mgr *service.Resources) (*reader, error) {
	r := &reader{
		log:          mgr.Logger(),
		shutSig:      shutdown.NewSignaller(),
		mgr:          mgr,
		checkpointer: checkpoint.NewCapped[string](1024),
	}
	var err error
	if r.channelID, err = conf.FieldString("channel_id"); err != nil {
		return nil, err
	}
	if r.botToken, err = conf.FieldString("bot_token"); err != nil {
		return nil, err
	}
	if r.cache, err = conf.FieldString("cache"); err != nil {
		return nil, err
	}
	if r.cacheKey, err = conf.FieldString("cache_key"); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *reader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.msgChan != nil {
		return nil
	}

	// Obtain the newest message we've already seen.
	var lastMsgID string
	var cacheErr error
	err := r.mgr.AccessCache(ctx, r.cache, func(c service.Cache) {
		var lastMsgIDBytes []byte
		if lastMsgIDBytes, cacheErr = c.Get(ctx, r.cacheKey); errors.Is(cacheErr, service.ErrKeyNotFound) {
			cacheErr = nil
		}
		lastMsgID = string(lastMsgIDBytes)
	})
	if err == nil {
		err = cacheErr
	}
	if err != nil {
		return fmt.Errorf("failed to obtain latest seen message ID: %v", err)
	}

	sess, doneWithSessFn, err := getGlobalSession(r.botToken)
	if err != nil {
		return err
	}

	msgChan := make(chan *discordgo.Message)
	go func() {
		defer func() {
			doneWithSessFn()
			r.shutSig.ShutdownComplete()
		}()

		backfill := func(beforeID, afterID string) string {
			for {
				if r.shutSig.ShouldCloseAtLeisure() {
					return ""
				}
				msgs, err := sess.ChannelMessages(r.channelID, 100, beforeID, afterID, "")
				if err != nil {
					r.log.Errorf("Failed to poll backlog of messages: %v", err)
				}
				for len(msgs) > 0 && msgs[0].ID == beforeID {
					msgs = msgs[1:]
				}
				if len(msgs) == 0 {
					return afterID
				}
				for i := len(msgs) - 1; i >= 0; i-- {
					afterID = msgs[i].ID
					select {
					case msgChan <- msgs[i]:
					case <-r.shutSig.CloseAtLeisureChan():
						return ""
					}
				}
			}
		}

		// First perform a backfill
		var lastSeen string
		if lastMsgID != "" {
			lastSeen = backfill("", lastMsgID)
		}
		if r.shutSig.ShouldCloseAtLeisure() {
			return
		}

		// Now listen for new messages. Note: There's a small chance here that
		// messages are delivered between our backfill and this handler being
		// registered, so on the first message we trigger _another_ backfill
		// just in case.
		triggeredMiniBackfill := false
		defer sess.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
			if m.ChannelID != r.channelID {
				return
			}
			if !triggeredMiniBackfill {
				triggeredMiniBackfill = true
				if lastSeen != "" {
					_ = backfill(m.ID, lastSeen)
				}
			}
			select {
			case <-r.shutSig.CloseAtLeisureChan():
				return
			case msgChan <- m.Message:
			}
		})()

		<-r.shutSig.CloseAtLeisureChan()
	}()

	r.msgChan = msgChan
	return nil
}

func (r *reader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.connMut.Lock()
	msgChan := r.msgChan
	r.connMut.Unlock()
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var msgEvent *discordgo.Message
	select {
	case msgEvent = <-msgChan:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	jBytes, err := json.Marshal(msgEvent)
	if err != nil {
		return nil, nil, err
	}

	release, err := r.checkpointer.Track(ctx, msgEvent.ID, 1)
	if err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(jBytes)
	return msg, func(ctx context.Context, err error) error {
		highestID := release()
		if highestID == nil {
			return nil
		}
		var setErr error
		if err := r.mgr.AccessCache(ctx, r.cache, func(c service.Cache) {
			setErr = c.Set(ctx, r.cacheKey, []byte(*highestID), nil)
		}); err != nil {
			return err
		}
		return setErr
	}, nil
}

func (r *reader) Close(ctx context.Context) error {
	go func() {
		r.shutSig.CloseAtLeisure()
		r.connMut.Lock()
		if r.msgChan == nil {
			// Indicates that we were never connected, so indicate shutdown is
			// complete.
			r.shutSig.ShutdownComplete()
		}
		r.connMut.Unlock()
	}()
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
