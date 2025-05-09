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

package spicedb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Jeffail/shutdown"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/dustin/go-humanize"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ service.Input = &watchInput{}

func init() {
	err := service.RegisterInput("spicedb_watch", watchInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return newWatchInput(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

func watchInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "SpiceDB").
		Summary(`Consume messages from the Watch API from SpiceDB.`).
		Description(`
The SpiceDB input allows you to consume messages from the Watch API of a SpiceDB instance.
This input is useful for applications that need to react to changes in the data managed by SpiceDB in real-time.

== Credentials

You need to provide the endpoint of your SpiceDB instance and a Bearer token for authentication.

== Cache

The zed token of the newest update consumed and acked is stored in a cache in order to start reading from it each time the input is initialised.
Ideally this cache should be persisted across restarts.
`).
		Fields(
			service.NewURLField("endpoint").
				Description("The SpiceDB endpoint.").
				Example("grpc.authzed.com:443"),
			service.NewStringField("bearer_token").
				Description("The SpiceDB Bearer token used to authenticate against the SpiceDB instance.").
				Default("").
				Example("t_your_token_here_1234567deadbeef").
				Secret(),
			service.NewStringField("max_receive_message_bytes").
				Description("Maximum message size in bytes the SpiceDB client can receive.").
				Advanced().
				Default("4MB").
				Example("100MB").
				Example("50mib"),
			service.NewStringField("cache").
				Description("A cache resource to use for performing unread message backfills, the ID of the last message received will be stored in this cache and used for subsequent requests."),
			service.NewStringField("cache_key").
				Description("The key identifier used when storing the ID of the last message received.").
				Default("authzed.com/spicedb/watch/last_zed_token").
				Advanced(),
			service.NewTLSToggledField("tls"),
		)
}

type watchMsg struct {
	msg *v1.WatchResponse
	err error
}

type watchInput struct {
	logger  *service.Logger
	shutSig *shutdown.Signaller
	mgr     *service.Resources

	clientConfig clientConfig
	cache        string
	cacheKey     string

	connMut sync.Mutex
	msgChan chan *watchMsg
}

func newWatchInput(pConf *service.ParsedConfig, mgr *service.Resources) (*watchInput, error) {
	in := &watchInput{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
		mgr:     mgr,
	}
	var err error
	if in.clientConfig.endpoint, err = pConf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if in.clientConfig.bearerToken, err = pConf.FieldString("bearer_token"); err != nil {
		return nil, err
	}
	var maxReceiveMessageBytesStr string
	if maxReceiveMessageBytesStr, err = pConf.FieldString("max_receive_message_bytes"); err != nil {
		return nil, err
	}
	if maxReceiveMessageSizeInBytes, err := humanize.ParseBytes(maxReceiveMessageBytesStr); err != nil {
		return nil, err
	} else {
		in.clientConfig.maxReceiveMessageSizeInBytes = int(maxReceiveMessageSizeInBytes)
	}
	if in.clientConfig.tlsConf, _, err = pConf.FieldTLSToggled("tls"); err != nil {
		return nil, err
	}
	if in.cache, err = pConf.FieldString("cache"); err != nil {
		return nil, err
	}
	if in.cacheKey, err = pConf.FieldString("cache_key"); err != nil {
		return nil, err
	}

	return in, nil
}

// Connect implements service.Input.
func (wi *watchInput) Connect(ctx context.Context) error {
	// 1. check if we are already connected
	wi.connMut.Lock()
	defer wi.connMut.Unlock()
	if wi.msgChan != nil {
		return nil
	}
	// 2. initialize spicedb connection
	client, err := wi.clientConfig.loadSpiceDBClient()
	if err != nil {
		return fmt.Errorf("failed to initialize SpiceDB client: %v", err)
	}

	// 3. get the last processed Zed token
	var (
		lastZedToken string
		startCursor  *v1.ZedToken
		cacheErr     error
	)
	err = wi.mgr.AccessCache(ctx, wi.cache, func(c service.Cache) {
		var lastZedTokenBytes []byte
		if lastZedTokenBytes, cacheErr = c.Get(ctx, wi.cacheKey); errors.Is(cacheErr, service.ErrKeyNotFound) {
			cacheErr = nil
		}
		lastZedToken = string(lastZedTokenBytes)
	})
	if err == nil {
		err = cacheErr
	}
	if err != nil {
		return fmt.Errorf("failed to obtain latest processed zed token: %v", err)
	}
	if lastZedToken != "" {
		startCursor = &v1.ZedToken{
			Token: lastZedToken,
		}
	}
	// 4. start the watch
	wi.msgChan = make(chan *watchMsg)
	go func() {
		defer wi.shutSig.TriggerHasStopped()
		ctx, cancel := wi.shutSig.SoftStopCtx(ctx)
		defer cancel()
		stream, err := client.Watch(ctx, &v1.WatchRequest{
			OptionalStartCursor: startCursor,
		})
		if err != nil {
			wi.logger.Errorf("unable to watch service: %s", err)
			return
		}
		for {
			if wi.shutSig.IsSoftStopSignalled() {
				return
			}
			watchResp, err := stream.Recv()
			if err == io.EOF {
				wi.logger.Infof("end of the watch stream")
				return
			}
			if err != nil {
				wi.logger.Errorf("unable to watch stream: %s", err)
				select {
				case wi.msgChan <- &watchMsg{err: err}:
				case <-wi.shutSig.SoftStopChan():
				}
				// If we encounter an error, we should stop the watch.
				return
			}
			select {
			case wi.msgChan <- &watchMsg{msg: watchResp}:
			case <-wi.shutSig.SoftStopChan():
				return
			}
		}
	}()

	return nil
}

// Read implements service.Input.
func (wi *watchInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	wi.connMut.Lock()
	defer wi.connMut.Unlock()

	if wi.msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var watchMsg *watchMsg
	select {
	case watchMsg = <-wi.msgChan:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	if watchMsg.err != nil {
		return nil, nil, watchMsg.err
	}
	msgBytes, err := protojson.Marshal(watchMsg.msg)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to marshal watch response: %w", err)
	}
	msg := service.NewMessage(msgBytes)
	return msg, func(ctx context.Context, err error) error {
		var setErr error
		if err := wi.mgr.AccessCache(ctx, wi.cache, func(c service.Cache) {
			setErr = c.Set(ctx, wi.cacheKey, []byte(watchMsg.msg.ChangesThrough.Token), nil)
		}); err != nil {
			return err
		}
		return setErr
	}, nil
}

// Close implements service.Input.
func (wi *watchInput) Close(ctx context.Context) error {
	go func() {
		wi.shutSig.TriggerSoftStop()
		wi.connMut.Lock()
		if wi.msgChan == nil {
			// Indicates that we were never connected, so indicate shutdown is
			// complete.
			wi.shutSig.TriggerHasStopped()
		}
		wi.connMut.Unlock()
	}()
	select {
	case <-wi.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
