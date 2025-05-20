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

package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterInput(
		"redis_scan", redisScanInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newRedisScanInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
}

const (
	matchFieldName = "match"
	typeFieldName  = "type"
)

func redisScanInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Summary(`Scans the set of keys in the current selected database and gets their values, using the Scan and respective, typed, Get commands.`).
		Description(`Optionally, iterates only elements matching a blob-style pattern. For example:

- ` + "`*foo*`" + ` iterates only keys which contain ` + "`foo`" + ` in it.
- ` + "`foo*`" + ` iterates only keys starting with ` + "`foo`" + `.

With no type specified (default) this input generates a message for each key value pair in the legacy format:

` + "```json" + `
{"key":"foo","value":"bar"}
` + "```" + `
For ` + "`type`" + ` set to ` + "`string`" + ` this input generates a string for each key value pair in the following format:

` + "```json" + `
"foovalue"
` + "```" + `

The key is stored in the metadata field ` + "`key`" + `.

For ` + "`type`" + ` set to ` + "`hash`" + ` this input generates a message for each key fields pair in the following format:

` + "```json" + `
{"field1":"Hello","field2":"Hi"}
` + "```" + `

The key is stored in the metadata field ` + "`key`" + `.
`).
		Categories("Services").
		Version("4.27.0")

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	return spec.
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewStringField(matchFieldName).
			Description("Iterates only elements matching the optional glob-style pattern. By default, it matches all elements.").
			Example("*").
			Example("1*").
			Example("foo*").
			Example("foo").
			Example("*4*").
			Default("")).
		Field(service.NewStringEnumField(typeFieldName, "hash", "string").
			Description("The type of the Redis keys to scan.").
			Default("string").
			Optional())
}

func newRedisScanInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}
	match, err := conf.FieldString(matchFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", matchFieldName, err)
	}

	t, err := conf.FieldString(typeFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", typeFieldName, err)
	}

	r := &redisScanReader{
		client: client,
		match:  match,
		log:    mgr.Logger(),
		t:      t,
	}
	return r, nil
}

type redisScanReader struct {
	match  string
	client redis.UniversalClient
	iter   *redis.ScanIterator
	log    *service.Logger
	t      string
}

func (r *redisScanReader) Connect(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	if r.t == "" {
		r.iter = r.client.Scan(context.Background(), 0, r.match, 0).Iterator()
	} else {
		r.iter = r.client.ScanType(context.Background(), 0, r.match, 0, r.t).Iterator()
	}
	return r.iter.Err()
}

func (r *redisScanReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if r.iter.Next(ctx) {
		key := r.iter.Val()

		msg := service.NewMessage(nil)
		switch r.t {
		case "hash":
			res := r.client.HGetAll(ctx, key)
			if err := res.Err(); err != nil {
				return nil, nil, err
			}

			m := make(map[string]any, len(res.Val()))
			for k, v := range res.Val() {
				m[k] = v
			}

			msg.SetStructuredMut(m)
			msg.MetaSet("key", key)
		case "string":
			res := r.client.Get(ctx, key)
			if err := res.Err(); err != nil {
				return nil, nil, err
			}

			msg.SetStructuredMut(res.Val())
			msg.MetaSet("key", key)
		default:
			// legacy behavior
			res := r.client.Get(ctx, key)
			if err := res.Err(); err != nil {
				return nil, nil, err
			}

			msg.SetStructuredMut(map[string]any{
				"key":   key,
				"value": res.Val(),
			})
		}
		return msg, func(ctx context.Context, err error) error {
			return err
		}, nil
	}
	return nil, nil, service.ErrEndOfInput
}

func (r *redisScanReader) Close(ctx context.Context) (err error) {
	return r.client.Close()
}
