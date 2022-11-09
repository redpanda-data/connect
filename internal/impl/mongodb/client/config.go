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

// AuthMechanismProperties is a config struct for the authentication mechanism for MongoDB.
type AuthMechanismProperties struct {
	ServiceName          string `json:"service_name" yaml:"service_name"`
	CanonicalizeHostname bool   `json:"canonicalize_host_name" yaml:"canonicalize_host_name"`
	ServiceRealm         string `json:"service_realm" yaml:"service_realm"`
	ServiceHost          string `json:"service_host" yaml:"service_host"`
	AwsSessionToken      string `json:"aws_session_token" yaml:"aws_session_token"`
}

func (m *AuthMechanismProperties) toStringMap() map[string]string {
	auth := make(map[string]string)

	if m.AwsSessionToken != "" {
		auth["AWS_SESSION_TOKEN"] = m.AwsSessionToken
	}

	if m.CanonicalizeHostname {
		auth["CANONICALIZE_HOST_NAME"] = "true"
	}

	if m.ServiceHost != "" {
		auth["SERVICE_HOST"] = m.ServiceHost
	}

	if m.ServiceName != "" {
		auth["SERVICE_NAME"] = m.ServiceName
	}

	if m.ServiceRealm != "" {
		auth["SERVICE_REALM"] = m.ServiceRealm
	}

	if len(auth) == 0 {
		return nil
	}

	return auth
}

// Config is a config struct for a mongo connection.
type Config struct {
	URL                     string                  `json:"url" yaml:"url"`
	Database                string                  `json:"database" yaml:"database"`
	AuthSource              string                  `json:"auth_source" yaml:"auth_source"`
	AuthMechanism           string                  `json:"auth_mechanism" yaml:"auth_mechanism"`
	AuthMechanismProperties AuthMechanismProperties `json:"auth_mechanism_properties" yaml:"auth_mechanism_properties"`
	Collection              string                  `json:"collection" yaml:"collection"`
	Username                string                  `json:"username" yaml:"username"`
	Password                string                  `json:"password" yaml:"password"`
	PasswordSet             bool                    `json:"password_set" yaml:"password_set"`
	ConnectTimeout          string                  `json:"connect_timeout" yaml:"connect_timeout"`
	SocketTimeout           string                  `json:"socket_timeout" yaml:"socket_timeout"`
	ServerSelectionTimeout  string                  `json:"server_selection_timeout" yaml:"server_selection_timeout"`
	AppName                 string                  `json:"app_name" yaml:"app_name"`
	MaxPoolSize             uint64                  `json:"max_pool_size" yaml:"max_pool_size"`
	MinPoolSize             uint64                  `json:"min_pool_size" yaml:"min_pool_size"`
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
		URL:                    "",
		Database:               "",
		Collection:             "",
		Username:               "",
		Password:               "",
		PasswordSet:            false,
		AuthSource:             "",
		AuthMechanism:          "",
		AppName:                "",
		MaxPoolSize:            100,
		MinPoolSize:            0,
		ConnectTimeout:         "10s",
		SocketTimeout:          "30s",
		ServerSelectionTimeout: "30s",
	}
}

// Client returns a new mongodb client based on the configuration parameters.
func (m Config) Client() (*mongo.Client, error) {
	opt := options.Client().ApplyURI(m.URL)

	if m.ConnectTimeout == "" {
		opt.SetConnectTimeout(10 * time.Second)

	} else {
		var err error
		var connectTimeout time.Duration

		if connectTimeout, err = time.ParseDuration(m.ConnectTimeout); err != nil {
			return nil, fmt.Errorf("unable to parse connection timeout duration string: %w", err)
		}

		opt.SetConnectTimeout(connectTimeout)
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

	opt.SetMinPoolSize(m.MinPoolSize)
	opt.SetMaxPoolSize(m.MaxPoolSize)

	if m.Username != "" && m.Password != "" {
		creds := options.Credential{
			Username:                m.Username,
			Password:                m.Password,
			AuthSource:              m.AuthSource,
			PasswordSet:             m.PasswordSet,
			AuthMechanism:           m.AuthMechanism,
			AuthMechanismProperties: m.AuthMechanismProperties.toStringMap(),
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
		docs.FieldObject("auth_mechanism_properties", `Used to specify additional configuration options for certain mechanisms. For more information, see [MongoDB docs](https://docs.mongodb.com/manual/core/authentication-mechanisms/).`).Optional().Advanced().AtVersion("4.11.0").WithChildren(
			docs.FieldString("service_name", "The service name to use for GSSAPI authentication.").Optional(),
			docs.FieldBool("canonicalize_host_name", "Whether to canonicalize the host name for GSSAPI authentication.").Optional(),
			docs.FieldString("service_realm", "The service realm to use for GSSAPI authentication.").Optional(),
			docs.FieldString("service_host", "The service host to use for GSSAPI authentication. The service_host and canonicalize_host_name properties must not be used at the same time on Linux and Darwin systems.").Optional(),
			docs.FieldString("aws_session_token", "The AWS token for MONGODB-AWS authentication. This is optional and used for authentication with temporary credentials.").Optional(),
		),
		docs.FieldString("app_name", "The [appName](https://www.mongodb.com/docs/manual/reference/connection-string/#mongodb-urioption-urioption.appName) to use in the client connection.").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("connect_timeout", "Connect timeout while connecting to the database.").HasDefault("10s").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("socket_timeout", "Socket timeout while connecting to the database.").HasDefault("30s").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldString("server_selection_timeout", "Server selection timeout while connecting to the database.").HasDefault("30s").Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldInt("min_pool_size", "The minimum number of connections allowed in the driver's connection pool to each server.").HasDefault(0).Optional().Advanced().AtVersion("4.11.0"),
		docs.FieldInt("max_pool_size", "The maximum number of connections allowed in the driver's connection pool to each server.").HasDefault(100).Optional().Advanced().AtVersion("4.11.0"),
	}
}
