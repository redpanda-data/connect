package redis

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Jeffail/benthos/v3/internal/docs"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/go-redis/redis/v7"
)

// Config is a config struct for a redis connection.
type Config struct {
	URL string      `json:"url" yaml:"url"`
	TLS btls.Config `json:"tls" yaml:"tls"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		URL: "tcp://localhost:6379",
		TLS: btls.NewConfig(),
	}
}

// Client returns a new redis client based on the configuration parameters.
func (r Config) Client() (*redis.Client, error) {
	url, err := url.Parse(r.URL)
	if err != nil {
		return nil, err
	}

	var pass string
	if url.User != nil {
		pass, _ = url.User.Password()
	}

	// We default to Redis DB 0 for backward compatibility, but if it's
	// specified in the URL, we'll use the specified one instead.
	var redisDB int
	if len(url.Path) > 1 {
		var err error
		// We'll strip the leading '/'
		redisDB, err = strconv.Atoi(url.Path[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid Redis DB, can't parse '%s'", url.Path)
		}
	}

	var tlsConf *tls.Config = nil
	if r.TLS.Enabled {
		var err error
		if tlsConf, err = r.TLS.Get(); err != nil {
			return nil, err
		}
	}

	return redis.NewClient(&redis.Options{
		Addr:      url.Host,
		Network:   url.Scheme,
		DB:        redisDB,
		Password:  pass,
		TLSConfig: tlsConf,
	}), nil
}

// ConfigDocs returns a documentation field spec for fields within a Config.
func ConfigDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon(
			"url", "The URL of the target Redis server. Database is optional and is supplied as the URL path.",
			"tcp://localhost:6379", "tcp://localhost:6379/1",
		),
		btls.FieldSpec(),
	}
}
