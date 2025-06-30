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

package nats

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// I've split the connection fields into two, which allows us to put tls and
// auth further down the fields stack. This is literally just polish for the
// docs.
func connectionHeadFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"}),
		service.NewIntField("max_reconnects").
			Description("The maximum number of times to attempt to reconnect to the server. If negative, it will never stop trying to reconnect.").
			Optional().
			Advanced(),
	}
}

func connectionTailFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewTLSToggledField("tls"),
		service.NewBoolField("tls_handshake_first").
			Description("Perform a TLS handshake before sending the INFO protocol message.").
			Optional().
			Default(false).
			Advanced(),
		authFieldSpec(),
	}
}

type connectionDetails struct {
	label             string
	logger            *service.Logger
	tlsConf           *tls.Config
	authConf          authConfig
	fs                *service.FS
	urls              string
	maxReconnects     *int
	tlsHandshakeFirst *bool
}

func connectionDetailsFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (c connectionDetails, err error) {
	c.label = mgr.Label()
	c.fs = mgr.FS()
	c.logger = mgr.Logger()

	var urlList []string
	if urlList, err = conf.FieldStringList("urls"); err != nil {
		return
	}
	c.urls = strings.Join(urlList, ",")

	if conf.Contains("max_reconnects") {
		if maxReconnects, err := conf.FieldInt("max_reconnects"); err != nil {
			return c, err
		} else {
			c.maxReconnects = &maxReconnects
		}
	}

	if conf.Contains("tls_handshake_first") {
		if tlsHandshakeFirst, err := conf.FieldBool("tls_handshake_first"); err != nil {
			return c, err
		} else {
			c.tlsHandshakeFirst = &tlsHandshakeFirst
		}
	}

	var tlsEnabled bool
	if c.tlsConf, tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return
	}
	if !tlsEnabled {
		c.tlsConf = nil
	}

	if c.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return
	}
	return
}

func (c *connectionDetails) get(_ context.Context, extraOpts ...nats.Option) (*nats.Conn, error) {
	var opts []nats.Option
	if c.tlsConf != nil {
		opts = append(opts, nats.Secure(c.tlsConf))
	}
	if c.tlsHandshakeFirst != nil && *c.tlsHandshakeFirst {
		opts = append(opts, nats.TLSHandshakeFirst())
	}
	opts = append(opts, nats.Name(c.label))
	opts = append(opts, errorHandlerOption(c.logger))
	opts = append(opts, authConfToOptions(c.authConf, c.fs)...)
	if c.maxReconnects != nil {
		opts = append(opts, nats.MaxReconnects(*c.maxReconnects))
	}
	opts = append(opts, extraOpts...)
	return nats.Connect(c.urls, opts...)
}
