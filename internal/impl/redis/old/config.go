package old

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/go-redis/redis/v7"
)

// Config is a config struct for a redis connection.
type Config struct {
	URL    string      `json:"url" yaml:"url"`
	Kind   string      `json:"kind" yaml:"kind"`
	Master string      `json:"master" yaml:"master"`
	TLS    btls.Config `json:"tls" yaml:"tls"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		URL:  "",
		Kind: "simple",
		TLS:  btls.NewConfig(),
	}
}

// Client returns a new redis client based on the configuration parameters.
func (r Config) Client() (redis.UniversalClient, error) {

	// We default to Redis DB 0 for backward compatibility
	var redisDB int
	var pass string
	var addrs []string

	// handle comma-separated urls
	for _, v := range strings.Split(r.URL, ",") {
		url, err := url.Parse(v)
		if err != nil {
			return nil, err
		}

		if url.Scheme == "tcp" {
			url.Scheme = "redis"
		}

		rurl, err := redis.ParseURL(url.String())
		if err != nil {
			return nil, err
		}

		addrs = append(addrs, rurl.Addr)
		redisDB = rurl.DB
		pass = rurl.Password
	}

	var tlsConf *tls.Config
	if r.TLS.Enabled {
		var err error
		if tlsConf, err = r.TLS.Get(); err != nil {
			return nil, err
		}
	}

	var client redis.UniversalClient
	var err error

	opts := &redis.UniversalOptions{
		Addrs:     addrs,
		DB:        redisDB,
		Password:  pass,
		TLSConfig: tlsConf,
	}

	switch r.Kind {
	case "simple":
		client = redis.NewClient(opts.Simple())
	case "cluster":
		client = redis.NewClusterClient(opts.Cluster())
	case "failover":
		opts.MasterName = r.Master
		client = redis.NewFailoverClient(opts.Failover())
	default:
		err = fmt.Errorf("invalid redis kind: %s", r.Kind)
	}

	return client, err
}

// ConfigDocs returns a documentation field spec for fields within a Config.
func ConfigDocs() docs.FieldSpecs {
	tlsSpec := btls.FieldSpec()
	tlsSpec.Description = tlsSpec.Description + `

**Troubleshooting**

Some cloud hosted instances of Redis (such as Azure Cache) might need some hand holding in order to establish stable connections. Unfortunately, it is often the case that TLS issues will manifest as generic error messages such as "i/o timeout". If you're using TLS and are seeing connectivity problems consider setting ` + "`enable_renegotiation` to `true`" + `, and ensuring that the server supports at least TLS version 1.2.`
	return docs.FieldSpecs{
		docs.FieldCommon(
			"url", "The URL of the target Redis server. Database is optional and is supplied as the URL path. The scheme `tcp` is equivalent to `redis`.",
			":6397",
			"localhost:6397",
			"redis://localhost:6379",
			"redis://:foopassword@redisplace:6379",
			"redis://localhost:6379/1",
			"redis://localhost:6379/1,redis://localhost:6380/1",
		),
		docs.FieldAdvanced("kind", "Specifies a simple, cluster-aware, or failover-aware redis client.", "simple", "cluster", "failover"),
		docs.FieldAdvanced("master", "Name of the redis master when `kind` is `failover`", "mymaster"),
		tlsSpec,
	}
}
