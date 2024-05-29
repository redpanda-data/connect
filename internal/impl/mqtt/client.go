package mqtt

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	msFieldClientURLs              = "urls"
	msFieldClientClientID          = "client_id"
	msFieldClientDynClientIDSuffix = "dynamic_client_id_suffix"
	msFieldClientConnectTimeout    = "connect_timeout"
	msFieldClientWill              = "will"
	msFieldClientWillEnabled       = "enabled"
	msFieldClientWillQoS           = "qos"
	msFieldClientWillRetained      = "retained"
	msFieldClientWillTopic         = "topic"
	msFieldClientWillPayload       = "payload"
	msFieldClientUser              = "user"
	msFieldClientPassword          = "password"
	msFieldClientKeepAlive         = "keepalive"
	msFieldClientTLS               = "tls"
)

func ClientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLListField(msFieldClientURLs).
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"tcp://localhost:1883"}),
		service.NewStringField(msFieldClientClientID).
			Description("An identifier for the client connection.").
			Default(""),
		service.NewStringAnnotatedEnumField(msFieldClientDynClientIDSuffix, map[string]string{
			"nanoid": "append a nanoid of length 21 characters",
		}).
			Description("Append a dynamically generated suffix to the specified `client_id` on each run of the pipeline. This can be useful when clustering Benthos producers.").
			Optional().
			Advanced().
			LintRule(`root = []`), // Disable linting for now
		service.NewDurationField(msFieldClientConnectTimeout).
			Description("The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.").
			Default("30s").
			Version("3.58.0").
			Examples("1s", "500ms"),
		service.NewObjectField(msFieldClientWill,
			service.NewBoolField(msFieldClientWillEnabled).
				Description("Whether to enable last will messages.").
				Default(false),
			service.NewIntField(msFieldClientWillQoS).
				Description("Set QoS for last will message. Valid values are: 0, 1, 2.").
				Default(0),
			service.NewBoolField(msFieldClientWillRetained).
				Description("Set retained for last will message.").
				Default(false),
			service.NewStringField(msFieldClientWillTopic).
				Description("Set topic for last will message.").
				Default(""),
			service.NewStringField(msFieldClientWillPayload).
				Description("Set payload for last will message.").
				Default(""),
		).
			Description("Set last will message in case of Benthos failure").
			Advanced(),
		service.NewStringField(msFieldClientUser).
			Description("A username to connect with.").
			Default("").
			Advanced(),
		service.NewStringField(msFieldClientPassword).
			Description("A password to connect with.").
			Default("").
			Secret().
			Advanced(),
		service.NewIntField(msFieldClientKeepAlive).
			Description("Max seconds of inactivity before a keepalive message is sent.").
			Default(30).
			Advanced(),
		service.NewTLSToggledField(msFieldClientTLS),
	}
}

type clientOptsBuilder struct {
	urls           []*url.URL
	clientID       string
	connectTimeout time.Duration
	keepAlive      int
	username       string
	password       string
	tlsEnabled     bool
	tlsConf        *tls.Config
	will           willOpt
}

func clientOptsFromParsed(conf *service.ParsedConfig) (opts clientOptsBuilder, err error) {
	if opts.urls, err = conf.FieldURLList(msFieldClientURLs); err != nil {
		return
	}
	if opts.clientID, err = conf.FieldString(msFieldClientClientID); err != nil {
		return
	}
	if conf.Contains(msFieldClientDynClientIDSuffix) {
		var tmpDynClientIDSuffix string
		if tmpDynClientIDSuffix, err = conf.FieldString(msFieldClientDynClientIDSuffix); err != nil {
			return
		}
		switch tmpDynClientIDSuffix {
		case "nanoid":
			var nid string
			if nid, err = gonanoid.New(); err != nil {
				err = fmt.Errorf("failed to generate nanoid: %w", err)
				return
			}
			opts.clientID += nid
		case "":
		default:
			err = fmt.Errorf("unknown dynamic_client_id_suffix: %v", tmpDynClientIDSuffix)
			return
		}
	}
	if opts.connectTimeout, err = conf.FieldDuration(msFieldClientConnectTimeout); err != nil {
		return
	}
	if opts.keepAlive, err = conf.FieldInt(msFieldClientKeepAlive); err != nil {
		return
	}
	if opts.username, err = conf.FieldString(msFieldClientUser); err != nil {
		return
	}
	if opts.password, err = conf.FieldString(msFieldClientPassword); err != nil {
		return
	}
	if opts.will, err = willOptFromParsed(conf.Namespace(msFieldClientWill)); err != nil {
		return
	}
	if opts.tlsConf, opts.tlsEnabled, err = conf.FieldTLSToggled(msFieldClientTLS); err != nil {
		return
	}
	return
}

func (b *clientOptsBuilder) apply(opts *mqtt.ClientOptions) *mqtt.ClientOptions {
	opts = opts.SetAutoReconnect(false).
		SetClientID(b.clientID).
		SetConnectTimeout(b.connectTimeout).
		SetKeepAlive(time.Duration(b.keepAlive) * time.Second)

	opts = b.will.apply(opts)

	if b.tlsEnabled {
		opts = opts.SetTLSConfig(b.tlsConf)
	}

	opts = opts.SetUsername(b.username)
	opts = opts.SetPassword(b.password)

	for _, u := range b.urls {
		opts = opts.AddBroker(u.String())
	}

	return opts
}

func willOptFromParsed(conf *service.ParsedConfig) (opt willOpt, err error) {
	if opt.Enabled, err = conf.FieldBool(msFieldClientWillEnabled); err != nil {
		return
	}

	var tmpQoS int
	if tmpQoS, err = conf.FieldInt(msFieldClientWillQoS); err != nil {
		return
	}
	opt.QoS = uint8(tmpQoS)

	if opt.Retained, err = conf.FieldBool(msFieldClientWillRetained); err != nil {
		return
	}

	if opt.Topic, err = conf.FieldString(msFieldClientWillTopic); err != nil {
		return
	}

	if opt.Payload, err = conf.FieldString(msFieldClientWillPayload); err != nil {
		return
	}

	if opt.Enabled && opt.Topic == "" {
		err = errors.New("include topic to register a last will")
		return
	}
	return
}

type willOpt struct {
	Enabled  bool
	QoS      uint8
	Retained bool
	Topic    string
	Payload  string
}

func (w *willOpt) apply(opts *mqtt.ClientOptions) *mqtt.ClientOptions {
	if !w.Enabled {
		return opts
	}
	opts = opts.SetWill(w.Topic, w.Payload, w.QoS, w.Retained)
	return opts
}
