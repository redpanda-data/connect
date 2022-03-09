package amqp1

import (
	"fmt"

	"github.com/Azure/go-amqp"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// ErrSASLMechanismNotSupported is returned if a SASL mechanism was not recognized.
type ErrSASLMechanismNotSupported string

// Error implements the standard error interface.
func (e ErrSASLMechanismNotSupported) Error() string {
	return fmt.Sprintf("SASL mechanism %v was not recognised", string(e))
}

// SASLConfig contains configuration for SASL based authentication.
type SASLConfig struct {
	Mechanism string `json:"mechanism" yaml:"mechanism"`
	User      string `json:"user" yaml:"user"`
	Password  string `json:"password" yaml:"password"`
}

// NewSASLConfig returns a new SASL config for AMQP with default values.
func NewSASLConfig() SASLConfig {
	return SASLConfig{
		Mechanism: "none",
	}
}

// SASLFieldSpec returns specs for SASL fields.
func SASLFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced("sasl", "Enables SASL authentication.").WithChildren(
		docs.FieldCommon("mechanism", "The SASL authentication mechanism to use.").HasAnnotatedOptions(
			"none", "No SASL based authentication.",
			"plain", "Plain text SASL authentication.",
		),
		docs.FieldCommon("user", "A SASL plain text username. It is recommended that you use environment variables to populate this field.", "${USER}"),
		docs.FieldCommon("password", "A SASL plain text password. It is recommended that you use environment variables to populate this field.", "${PASSWORD}"),
	)
}

// ToOptFns renders the sasl.Config options into amqp.ConnOption fns.
func (s SASLConfig) ToOptFns() ([]amqp.ConnOption, error) {
	switch s.Mechanism {
	case "plain":
		return []amqp.ConnOption{
			amqp.ConnSASLPlain(s.User, s.Password),
		}, nil
	case "none":
		return nil, nil
	}
	return nil, ErrSASLMechanismNotSupported(s.Mechanism)
}
