package old

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
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

// ConfigDocs returns a documentation field spec for fields within a Config.
func ConfigDocs() docs.FieldSpecs {
	tlsSpec := btls.FieldSpec()
	tlsSpec.Description = tlsSpec.Description + `

**Troubleshooting**

Some cloud hosted instances of Redis (such as Azure Cache) might need some hand holding in order to establish stable connections. Unfortunately, it is often the case that TLS issues will manifest as generic error messages such as "i/o timeout". If you're using TLS and are seeing connectivity problems consider setting ` + "`enable_renegotiation` to `true`" + `, and ensuring that the server supports at least TLS version 1.2.`
	return docs.FieldSpecs{
		docs.FieldString(
			"url", "The URL of the target Redis server. Database is optional and is supplied as the URL path. The scheme `tcp` is equivalent to `redis`.",
			":6397",
			"localhost:6397",
			"redis://localhost:6379",
			"redis://:foopassword@redisplace:6379",
			"redis://localhost:6379/1",
			"redis://localhost:6379/1,redis://localhost:6380/1",
		).HasDefault(""),
		docs.FieldString("kind", "Specifies a simple, cluster-aware, or failover-aware redis client.", "simple", "cluster", "failover").HasDefault("simple").Advanced(),
		docs.FieldString("master", "Name of the redis master when `kind` is `failover`", "mymaster").HasDefault("").Advanced(),
		tlsSpec,
	}
}
