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

package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
)

const (
	// Connection fields
	kfcFieldSeedBrokers            = "seed_brokers"
	kfcFieldClientID               = "client_id"
	kfcFieldTLS                    = "tls"
	kfcFieldMetadataMaxAge         = "metadata_max_age"
	kfcFieldRequestTimeoutOverhead = "request_timeout_overhead"
	kfcFieldConnIdleTimeout        = "conn_idle_timeout"

	kfcFieldSeedBrokersDescription = "A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas, it is expanded into multiple addresses."
)

// FranzConnectionOptionalFields returns a slice of connection fields but
// with any non-optional fields switched to be optional.
func FranzConnectionOptionalFields() []*service.ConfigField {
	fields := FranzConnectionFields()
	fields[0] = fields[0].
		Description(kfcFieldSeedBrokersDescription + "\n\nIf you omit this field, this component takes its entire connection configuration, including TLS and SASL settings, from the top-level `redpanda` block, and ignores the connection fields set on this component.").
		ShortDescription("Broker addresses used to establish connections. If omitted, the whole connection comes from the top-level redpanda block.").
		Optional()
	return fields
}

// FranzConnectionFields returns a slice of fields specifically for establishing
// connections to kafka brokers via the franz-go library.
func FranzConnectionFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(kfcFieldSeedBrokers).
			Description(kfcFieldSeedBrokersDescription).
			ShortDescription("Broker addresses used to establish connections. Items containing commas are expanded.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"}),
		service.NewStringField(kfcFieldClientID).
			Description("An identifier for the client connection. This identifier appears in broker logs and metrics, which helps you identify the Redpanda Connect instance that is connecting.").
			Default("redpanda-connect").
			Advanced(),
		service.NewTLSToggledField(kfcFieldTLS),
		SASLFields(),
		service.NewDurationField(kfcFieldMetadataMaxAge).
			Description("The maximum period of time after which metadata is refreshed. This field accepts Go duration format strings such as `100ms`, `1s`, or `5s`.\n\nLower values provide more responsive topic and partition discovery but may increase broker load. Higher values reduce broker queries but can delay detection of topology changes.\n\nThis interval also controls how frequently regex topic patterns are re-evaluated to discover new matching topics.").
			ShortDescription("Maximum age of metadata before it is refreshed. Also controls how often regex topic patterns are re-evaluated.").
			Default("1m").
			Advanced(),
		service.NewDurationField(kfcFieldRequestTimeoutOverhead).
			Description(`Additional time to apply as overhead when calculating request deadlines. For most requests, the deadline is this overhead alone. For requests that define their own timeout field, the overhead is added on top of that timeout, which helps prevent premature timeouts.

This field is roughly equivalent to Apache Kafka's ` + "`" + `request.timeout.ms` + "`" + ` parameter, but grants extra time to requests that have timeout fields.`).
			ShortDescription("Additional time granted when deadlining requests. Roughly equivalent to request.timeout.ms.").
			Default("10s").
			Advanced(),
		service.NewDurationField(kfcFieldConnIdleTimeout).
			Description("The approximate maximum duration that connections can remain idle before they are automatically closed. This field accepts Go duration format strings such as `100ms`, `1s`, or `5s`.").
			Default("20s").
			Advanced(),
		netutil.DialerConfigSpec(),
	}
}

// FranzConnectionDetails describes information required to create a kafka
// connection.
type FranzConnectionDetails struct {
	SeedBrokers            []string
	ClientID               string
	TLSEnabled             bool
	TLSConf                *tls.Config
	SASL                   []sasl.Mechanism
	MetaMaxAge             time.Duration
	RequestTimeoutOverhead time.Duration
	ConnIdleTimeout        time.Duration
	DialerConfig           netutil.DialerConfig

	Logger *service.Logger
}

// FranzConnectionDetailsFromConfig returns a summary of kafka connection
// information, which can be used in order to create a client.
func FranzConnectionDetailsFromConfig(conf *service.ParsedConfig, log *service.Logger) (*FranzConnectionDetails, error) {
	d := FranzConnectionDetails{
		Logger: log,
	}

	if conf.Contains(kfcFieldSeedBrokers) {
		brokerList, err := conf.FieldStringList(kfcFieldSeedBrokers)
		if err != nil {
			return nil, err
		}
		for _, b := range brokerList {
			d.SeedBrokers = append(d.SeedBrokers, strings.Split(b, ",")...)
		}
	}

	var err error
	if d.TLSConf, d.TLSEnabled, err = conf.FieldTLSToggled(kfcFieldTLS); err != nil {
		return nil, err
	}

	if d.SASL, err = SASLMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	if d.ClientID, err = conf.FieldString(kfcFieldClientID); err != nil {
		return nil, err
	}

	if d.MetaMaxAge, err = conf.FieldDuration(kfcFieldMetadataMaxAge); err != nil {
		return nil, err
	}

	if d.RequestTimeoutOverhead, err = conf.FieldDuration(kfcFieldRequestTimeoutOverhead); err != nil {
		return nil, err
	}

	if d.ConnIdleTimeout, err = conf.FieldDuration(kfcFieldConnIdleTimeout); err != nil {
		return nil, err
	}

	if conf.Contains("tcp") {
		if d.DialerConfig, err = netutil.DialerConfigFromParsed(conf.Namespace("tcp")); err != nil {
			return nil, err
		}
	}

	return &d, nil
}

// IsConfigured returns true if any of the connection fields have been set.
func (d *FranzConnectionDetails) IsConfigured() bool {
	return len(d.SeedBrokers) > 0
}

// FranzOpts returns a slice of franz-go opts that establish a connection
// described in the connection details.
func (d *FranzConnectionDetails) FranzOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.WithLogger(&KGoLogger{d.Logger}),
		kgo.WithHooks(newConnHooks(d.Logger)),
		kgo.SeedBrokers(d.SeedBrokers...),
		kgo.SASL(d.SASL...),
		kgo.ClientID(d.ClientID),
		kgo.MetadataMaxAge(d.MetaMaxAge),
		kgo.RequestTimeoutOverhead(d.RequestTimeoutOverhead),
		kgo.ConnIdleTimeout(d.ConnIdleTimeout),
	}

	{
		var nd net.Dialer
		if err := netutil.DecorateDialer(&nd, d.DialerConfig); err != nil {
			d.Logger.Errorf("Failed to configure custom dialer: %v", err)
		} else {
			if d.TLSEnabled {
				opts = append(opts, kgo.Dialer((&tls.Dialer{
					NetDialer: &nd,
					Config:    d.TLSConf,
				}).DialContext))
			} else {
				opts = append(opts, kgo.Dialer(nd.DialContext))
			}
		}
	}

	return opts
}

// FranzConnectionOptsFromConfig returns a slice of franz-go client opts from a
// parsed config.
func FranzConnectionOptsFromConfig(conf *service.ParsedConfig, log *service.Logger) ([]kgo.Opt, error) {
	d, err := FranzConnectionDetailsFromConfig(conf, log)
	if err != nil {
		return nil, err
	}
	return d.FranzOpts(), nil
}

// NewFranzClient attempts to establish a new kafka client, and ensures that
// config errors such as invalid SASL credentials result in the client being
// closed and an error being returned instead of an endless retry loop.
func NewFranzClient(ctx context.Context, opts ...kgo.Opt) (*kgo.Client, error) {
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx); err != nil {
		client.Close()
		if !kgo.IsRetryableBrokerErr(err) {
			return nil, service.NewErrBackOff(err, time.Minute)
		}
		return nil, err
	}

	return client, nil
}
