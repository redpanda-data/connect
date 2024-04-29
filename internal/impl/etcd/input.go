package etcd

import (
	"context"
	"errors"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/public/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdInput struct {
	cli           *clientv3.Client
	key           string
	watchChan     clientv3.WatchChan
	interruptChan chan struct{}
	interruptOnce sync.Once
}

func newEtcdInputFromConfig(conf *service.ParsedConfig) (*etcdInput, error) {
	cli, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	var key string
	if conf.Contains("key") {
		if key, err = conf.FieldString("key"); err != nil {
			return nil, err
		}
	}

	return &etcdInput{
		cli:           cli,
		key:           key,
		interruptChan: make(chan struct{}),
	}, nil
}

func etcdInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Summary("Use etcd as a input and watch a key.")

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Field(service.NewStringField("key").
			Description("The key you want to watch.").
			Optional().
			Advanced())
	spec = spec.Field(service.NewAutoRetryNacksToggleField())
	return spec
}
func newEtcdInput(conf *service.ParsedConfig) (service.Input, error) {
	e, err := newEtcdInputFromConfig(conf)
	if err != nil {
		return nil, err
	}
	return service.AutoRetryNacks(e), err
}

func init() {
	err := service.RegisterInput(
		"etcd", etcdInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := newEtcdInput(conf)
			if err != nil {
				return nil, err
			}

			r, err := service.AutoRetryNacksToggled(conf, input)
			if err != nil {
				return nil, err
			}
			return span.NewInput("etcd", conf, r, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (e *etcdInput) Connect(ctx context.Context) error {
	watchChan := e.cli.Watch(ctx, e.key)
	e.watchChan = watchChan
	return nil
}
func (e *etcdInput) disconnect() {
	if e.cli.ActiveConnection().GetState().String() != "SHUTDOWN" {
		e.cli.Close()
		e.cli = nil
	}
	e.watchChan = nil
}

func (e *etcdInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {

	watchChan := e.watchChan
	var open bool
	var watchResp clientv3.WatchResponse
	select {
	case watchResp, open = <-watchChan:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case _, open = <-e.interruptChan:
	}

	if !open {
		e.disconnect()
		return nil, nil, service.ErrNotConnected
	}

	if len(watchResp.Events) > 1 {
		return nil, nil, errors.New("Multiple etcd events were received.")
	}

	event := watchResp.Events[0]
	message := service.NewMessage(nil)

	switch event.Type.String() {
	case "PUT":
		message.SetStructuredMut(map[string]any{
			"operation":       "PUT",
			"key":             string(event.Kv.Key),
			"value":           string(event.Kv.Value),
			"create_revision": event.Kv.CreateRevision,
			"mod_revision":    event.Kv.ModRevision,
		})
	case "DELETE":
		message.SetStructuredMut(map[string]any{
			"operation":    "DELETE",
			"key":          string(event.Kv.Key),
			"mod_revision": event.Kv.ModRevision,
		})
	}

	return message, func(ctx context.Context, err error) error {
		return err
	}, nil

}

func (e *etcdInput) Close(ctx context.Context) error {
	go func() {
		e.disconnect()
	}()
	e.interruptOnce.Do(func() {
		close(e.interruptChan)
	})
	return nil
}
