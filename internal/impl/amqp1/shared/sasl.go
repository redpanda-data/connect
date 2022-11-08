// Package shared contains docs fields that need to be shared across old and new
// component implementations, it needs to be separate from the parent package in
// order to avoid circular dependencies (for now).
package shared

import (
	"fmt"

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
	return docs.FieldObject("sasl", "Enables SASL authentication.").WithChildren(
		docs.FieldString("mechanism", "The SASL authentication mechanism to use.").HasAnnotatedOptions(
			"none", "No SASL based authentication.",
			"plain", "Plain text SASL authentication.",
		).HasDefault("none"),
		docs.FieldString("user", "A SASL plain text username. It is recommended that you use environment variables to populate this field.", "${USER}").HasDefault(""),
		docs.FieldString("password", "A SASL plain text password. It is recommended that you use environment variables to populate this field.", "${PASSWORD}").HasDefault("").Secret(),
	).Advanced()
}
