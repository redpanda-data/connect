package sasl

import (
	"fmt"

	"github.com/Azure/go-amqp"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

// ErrMechanismNotSupported is returned if a SASL mechanism was not recognized.
type ErrMechanismNotSupported string

// Error implements the standard error interface.
func (e ErrMechanismNotSupported) Error() string {
	return fmt.Sprintf("SASL mechanism %v was not recognised", string(e))
}

// Config contains configuration for SASL based authentication.
type Config struct {
	Mechanism string `json:"mechanism" yaml:"mechanism"`
	User      string `json:"user" yaml:"user"`
	Password  string `json:"password" yaml:"password"`
}

// NewConfig returns a new SASL config for AMQP with default values.
func NewConfig() Config {
	return Config{
		Mechanism: "none",
	}
}

// FieldSpec returns specs for SASL fields.
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("sasl", "Enables SASL authentication.").WithChildren(
		docs.FieldCommon("mechanism", "The SASL authentication mechanism to use.").HasOptions("none", "plain"),
		docs.FieldCommon("user", "A SASL plain text username. It is recommended that you use environment variables to populate this field.", "${USER}"),
		docs.FieldCommon("password", "A SASL plain text password. It is recommended that you use environment variables to populate this field.", "${PASSWORD}"),
	)
}

// ToOptFns renders the sasl.Config options into amqp.ConnOption fns.
func (s Config) ToOptFns() ([]amqp.ConnOption, error) {
	switch s.Mechanism {
	case "plain":
		return []amqp.ConnOption{
			amqp.ConnSASLPlain(s.User, s.Password),
		}, nil
	case "none":
		return nil, nil
	}
	return nil, ErrMechanismNotSupported(s.Mechanism)
}
