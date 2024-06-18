package cassandra

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cFieldAddresses           = "addresses"
	cFieldTLS                 = "tls"
	cFieldPassAuth            = "password_authenticator"
	cFieldPassAuthEnabled     = "enabled"
	cFieldPassAuthUsername    = "username"
	cFieldPassAuthPassword    = "password"
	cFieldDisableIHL          = "disable_initial_host_lookup"
	cFieldMaxRetries          = "max_retries"
	cFieldBackoff             = "backoff"
	cFieldBackoffInitInterval = "initial_interval"
	cFieldBackoffMaxInterval  = "max_interval"
	cFieldTimeout             = "timeout"
)

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(cFieldAddresses).
			Description("A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.").
			Examples(
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			),
		service.NewTLSToggledField(cFieldTLS).Advanced(),
		service.NewObjectField(cFieldPassAuth,
			service.NewBoolField(cFieldPassAuthEnabled).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(cFieldPassAuthUsername).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(cFieldPassAuthPassword).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of Cassandra authentication parameters.").
			Advanced(),
		service.NewBoolField(cFieldDisableIHL).
			Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
			Advanced().
			Default(false),
		service.NewIntField(cFieldMaxRetries).
			Description("The maximum number of retries before giving up on a request.").
			Advanced().
			Default(3),
		service.NewObjectField(cFieldBackoff,
			service.NewDurationField(cFieldBackoffInitInterval).
				Description("The initial period to wait between retry attempts.").
				Default("1s"),
			service.NewDurationField(cFieldBackoffMaxInterval).
				Description("The maximum period to wait between retry attempts.").
				Default("5s"),
		).
			Description("Control time intervals between retry attempts.").
			Advanced(),
		service.NewDurationField(cFieldTimeout).
			Description("The client connection timeout.").
			Default("600ms"),
	}
}

type clientConf struct {
	addresses           []string
	tlsEnabled          bool
	tlsConf             *tls.Config
	authEnabled         bool
	authUsername        string
	authPassword        string
	disableIHL          bool
	maxRetries          int
	backoffInitInterval time.Duration
	backoffMaxInterval  time.Duration
	timeout             time.Duration
}

func (c *clientConf) Create() (*gocql.ClusterConfig, error) {
	conn := gocql.NewCluster(c.addresses...)
	if c.tlsEnabled {
		conn.SslOpts = &gocql.SslOptions{
			Config: c.tlsConf,
		}
		conn.DisableInitialHostLookup = c.tlsConf.InsecureSkipVerify
	} else {
		conn.DisableInitialHostLookup = c.disableIHL
	}

	if c.authEnabled {
		conn.Authenticator = gocql.PasswordAuthenticator{
			Username: c.authUsername,
			Password: c.authPassword,
		}
	}

	conn.RetryPolicy = &decorator{
		NumRetries: c.maxRetries,
		Min:        c.backoffInitInterval,
		Max:        c.backoffMaxInterval,
	}

	conn.Timeout = c.timeout
	return conn, nil
}

func clientConfFromParsed(conf *service.ParsedConfig) (c clientConf, err error) {
	var tmpAddresses []string
	if tmpAddresses, err = conf.FieldStringList(cFieldAddresses); err != nil {
		return
	}
	for _, a := range tmpAddresses {
		c.addresses = append(c.addresses, strings.Split(a, ",")...)
	}

	if c.tlsConf, c.tlsEnabled, err = conf.FieldTLSToggled(cFieldTLS); err != nil {
		return
	}

	{
		authConf := conf.Namespace(cFieldPassAuth)
		c.authEnabled, _ = authConf.FieldBool(cFieldPassAuthEnabled)
		c.authUsername, _ = authConf.FieldString(cFieldPassAuthUsername)
		c.authPassword, _ = authConf.FieldString(cFieldPassAuthPassword)
	}

	if c.disableIHL, err = conf.FieldBool(cFieldDisableIHL); err != nil {
		return
	}
	if c.maxRetries, err = conf.FieldInt(cFieldMaxRetries); err != nil {
		return
	}
	if c.backoffInitInterval, err = conf.FieldDuration(cFieldBackoff, cFieldBackoffInitInterval); err != nil {
		return
	}
	if c.backoffMaxInterval, err = conf.FieldDuration(cFieldBackoff, cFieldBackoffMaxInterval); err != nil {
		return
	}
	if c.timeout, err = conf.FieldDuration(cFieldTimeout); err != nil {
		return
	}
	return
}
