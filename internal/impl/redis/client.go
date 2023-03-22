package redis

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/public/service"
)

func clientFields() []*service.ConfigField {
	tlsField := service.NewTLSToggledField("tls").
		Description(`Custom TLS settings can be used to override system defaults.

**Troubleshooting**

Some cloud hosted instances of Redis (such as Azure Cache) might need some hand holding in order to establish stable connections. Unfortunately, it is often the case that TLS issues will manifest as generic error messages such as "i/o timeout". If you're using TLS and are seeing connectivity problems consider setting ` + "`enable_renegotiation` to `true`" + `, and ensuring that the server supports at least TLS version 1.2.`)

	return []*service.ConfigField{
		service.NewURLField("url").
			Description("The URL of the target Redis server. Database is optional and is supplied as the URL path.").
			Example(":6397").
			Example("localhost:6397").
			Example("redis://localhost:6379").
			Example("redis://:foopassword@redisplace:6379").
			Example("redis://localhost:6379/1").
			Example("redis://localhost:6379/1,redis://localhost:6380/1"),
		service.NewStringEnumField("kind", "simple", "cluster", "failover").
			Description("Specifies a simple, cluster-aware, or failover-aware redis client.").
			Default("simple").
			Advanced(),
		service.NewStringField("master").
			Description("Name of the redis master when `kind` is `failover`").
			Default("").
			Example("mymaster").
			Advanced(),
		tlsField,
	}
}

func getClient(parsedConf *service.ParsedConfig) (redis.UniversalClient, error) {
	urlStr, err := parsedConf.FieldString("url")
	if err != nil {
		return nil, err
	}

	kind, err := parsedConf.FieldString("kind")
	if err != nil {
		return nil, err
	}

	master, err := parsedConf.FieldString("master")
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := parsedConf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	// We default to Redis DB 0 for backward compatibility
	var redisDB int
	var pass string
	var addrs []string

	// handle comma-separated urls
	for _, v := range strings.Split(urlStr, ",") {
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

	var client redis.UniversalClient
	opts := &redis.UniversalOptions{
		Addrs:     addrs,
		DB:        redisDB,
		Password:  pass,
		TLSConfig: tlsConf,
	}

	switch kind {
	case "simple":
		client = redis.NewClient(opts.Simple())
	case "cluster":
		client = redis.NewClusterClient(opts.Cluster())
	case "failover":
		opts.MasterName = master
		client = redis.NewFailoverClient(opts.Failover())
	default:
		err = fmt.Errorf("invalid redis kind: %s", kind)
	}

	return client, err
}

func clientFromConfig(f ifs.FS, r old.Config) (redis.UniversalClient, error) {
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
		if tlsConf, err = r.TLS.Get(f); err != nil {
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
