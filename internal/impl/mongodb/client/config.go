package client

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Operation represents the operation that will be performed by MongoDB.
type Operation string

const (
	// OperationInsertOne Insert One operation.
	OperationInsertOne Operation = "insert-one"
	// OperationDeleteOne Delete One operation.
	OperationDeleteOne Operation = "delete-one"
	// OperationDeleteMany Delete many operation.
	OperationDeleteMany Operation = "delete-many"
	// OperationReplaceOne Replace one operation.
	OperationReplaceOne Operation = "replace-one"
	// OperationUpdateOne Update one operation.
	OperationUpdateOne Operation = "update-one"
	// OperationFindOne Find one operation.
	OperationFindOne Operation = "find-one"
	// OperationFindAndUpdate Find and update operation.
	OperationFindAndUpdate Operation = "find-and-update"
	// OperationInvalid Invalid operation.
	OperationInvalid Operation = "invalid"
)

// NewOperation converts a string operation to a strongly-typed Operation.
func NewOperation(op string) Operation {
	switch op {
	case "insert-one":
		return OperationInsertOne
	case "delete-one":
		return OperationDeleteOne
	case "delete-many":
		return OperationDeleteMany
	case "replace-one":
		return OperationReplaceOne
	case "update-one":
		return OperationUpdateOne
	case "find-one":
		return OperationFindOne
	case "find-and-update":
		return OperationFindAndUpdate
	default:
		return OperationInvalid
	}
}

// JSONMarshalMode represents the way in which BSON should be marshalled to JSON.
type JSONMarshalMode string

const (
	// JSONMarshalModeCanonical Canonical BSON to JSON marshal mode.
	JSONMarshalModeCanonical JSONMarshalMode = "canonical"
	// JSONMarshalModeRelaxed Relaxed BSON to JSON marshal mode.
	JSONMarshalModeRelaxed JSONMarshalMode = "relaxed"
)

// Config is a config struct for a mongo connection.
type Config struct {
	URL                     string            `json:"url" yaml:"url"`
	Database                string            `json:"database" yaml:"database"`
	AuthSource              string            `json:"auth_source" yaml:"auth_source"`
	AuthMechanism           string            `json:"auth_mecanism" yaml:"auth_mecanism"`
	AuthMechanismProperties map[string]string `json:"auth_mecanism_properties" yaml:"auth_mecanism_properties"`
	Collection              string            `json:"collection" yaml:"collection"`
	Username                string            `json:"username" yaml:"username"`
	Password                string            `json:"password" yaml:"password"`
	PasswordSet             bool              `json:"password_set" yaml:"password_set"`
	ConnectionTimeout       string            `json:"connection_timeout" yaml:"connection_timeout"`
	SocketTimeout           string            `json:"socket_timeout" yaml:"socket_timeout"`
	ServerSelectionTimeout  string            `json:"server_selection_timeout" yaml:"server_selection_timeout"`
	AppName                 string            `json:"app_name" yaml:"app_name"`
	MaxPoolSize             *uint64           `json:"max_pool_size" yaml:"max_pool_size"`
	MinPoolSize             *uint64           `json:"min_pool_size" yaml:"min_pool_size"`
}

// WriteConcern describes a write concern for MongoDB.
type WriteConcern struct {
	W        string `json:"w" yaml:"w"`
	J        bool   `json:"j" yaml:"j"`
	WTimeout string `json:"w_timeout" yaml:"w_timeout"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		URL:                     "",
		Database:                "",
		Collection:              "",
		Username:                "",
		Password:                "",
		PasswordSet:             false,
		AuthSource:              "",
		AuthMechanism:           "",
		AuthMechanismProperties: map[string]string{},
		AppName:                 "",
		ConnectionTimeout:       "10s",
		SocketTimeout:           "30s",
		ServerSelectionTimeout:  "30s",
	}
}

// Client returns a new mongodb client based on the configuration parameters.
func (m Config) Client() (*mongo.Client, error) {
	opt := options.Client().ApplyURI(m.URL)

	if m.ConnectionTimeout == "" {
		opt.SetConnectTimeout(10 * time.Second)

	} else {
		var err error
		var connectionTimeout time.Duration

		if connectionTimeout, err = time.ParseDuration(m.ConnectionTimeout); err != nil {
			return nil, fmt.Errorf("unable to parse connection timeout duration string: %w", err)
		}

		opt.SetConnectTimeout(connectionTimeout)
	}

	if m.SocketTimeout == "" {
		opt.SetSocketTimeout(30 * time.Second)

	} else {
		var err error
		var socketTimeout time.Duration

		if socketTimeout, err = time.ParseDuration(m.SocketTimeout); err != nil {
			return nil, fmt.Errorf("unable to parse socket timeout duration string: %w", err)
		}

		opt.SetSocketTimeout(socketTimeout)
	}

	if m.ServerSelectionTimeout == "" {
		opt.SetServerSelectionTimeout(30 * time.Second)

	} else {
		var err error
		var serverSelectionTimeout time.Duration

		if serverSelectionTimeout, err = time.ParseDuration(m.ServerSelectionTimeout); err != nil {
			return nil, fmt.Errorf("unable to parse server selection timeout duration string: %w", err)
		}

		opt.SetServerSelectionTimeout(serverSelectionTimeout)
	}

	if m.MinPoolSize != nil {
		opt.SetMinPoolSize(*m.MinPoolSize)
	}

	if m.MaxPoolSize != nil {
		opt.SetMaxPoolSize(*m.MaxPoolSize)
	}

	if m.Username != "" && m.Password != "" {
		creds := options.Credential{
			Username:                m.Username,
			Password:                m.Password,
			AuthSource:              m.AuthSource,
			PasswordSet:             m.PasswordSet,
			AuthMechanism:           m.AuthMechanism,
			AuthMechanismProperties: m.AuthMechanismProperties,
		}
		opt.SetAuth(creds)
	}

	if m.AppName != "" {
		opt.SetAppName(m.AppName)
	}

	client, err := mongo.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}

	return client, nil
}

// ConfigDocs returns a documentation field spec for fields within a Config.
func ConfigDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(
			"url", "The URL of the target MongoDB DB.",
			"mongodb://localhost:27017",
		),
		docs.FieldString("database", "The name of the target MongoDB DB."),
		docs.FieldString("username", "The username to connect to the database."),
		docs.FieldString("password", "The password to connect to the database."),
		docs.FieldBool("password_set", "For GSSAPI, this must be true if a password is specified, even if the password is the empty string, and false if no password is specified, indicating that the password should be taken from the context of the running process. For other mechanisms, this field is ignored.").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("auth_source", `The name of the database to use for authentication. This can also be set through the "authSource" URI option (e.g. "authSource=otherDb"). For more information, see [MongoDB docs](https://www.mongodb.com/docs/manual/reference/connection-string/#mongodb-urioption-urioption.authSource).`).Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("auth_mechanism", `The mechanism to use for authentication. This can also be set through the "authMechanism" URI option. (e.g. "authMechanism=PLAIN"). For more information, see [MongoDB docs](https://docs.mongodb.com/manual/core/authentication-mechanisms/).`).Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("app_name", "The [appName](https://www.mongodb.com/docs/manual/reference/connection-string/#mongodb-urioption-urioption.appName) to use in the client connection.").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("connect_timeout", "Connect timeout while connecting to the database.").HasDefault("10s").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("socket_timeout", "Socket timeout while connecting to the database.").HasDefault("30s").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("server_selection_timeout", "Server selection timeout while connecting to the database.").HasDefault("30s").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldInt("min_pool_size", "The minimum number of connections allowed in the driver's connection pool to each server.").HasDefault(0).Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldInt("max_pool_size", "The maximum number of connections allowed in the driver's connection pool to each server.").HasDefault(100).Optional().Advanced().AtVersion("4.11.0"),
	}
}
