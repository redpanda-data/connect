package amqp1

import (
	"fmt"

	"github.com/Azure/go-amqp"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Shared
	urlField      = "url"
	tlsField      = "tls"
	saslField     = "sasl"
	saslMechField = "mechanism"
	saslUserField = "user"
	saslPassField = "password"

	// Input
	sourceAddrField     = "source_address"
	azureRenewLockField = "azure_renew_lock"

	// Output
	targetAddrField  = "target_address"
	appPropsMapField = "application_properties_map"
	metaFilterField  = "metadata"
)

// ErrSASLMechanismNotSupported is returned if a SASL mechanism was not recognized.
type ErrSASLMechanismNotSupported string

// Error implements the standard error interface.
func (e ErrSASLMechanismNotSupported) Error() string {
	return fmt.Sprintf("SASL mechanism %v was not recognised", string(e))
}

func saslOptFnsFromParsed(conf *service.ParsedConfig) ([]amqp.ConnOption, error) {
	if !conf.Contains(saslField) {
		return nil, nil
	}
	conf = conf.Namespace(saslField)

	mechanism, err := conf.FieldString(saslMechField)
	if err != nil {
		return nil, err
	}

	user, err := conf.FieldString(saslUserField)
	if err != nil {
		return nil, err
	}

	pass, err := conf.FieldString(saslPassField)
	if err != nil {
		return nil, err
	}

	switch mechanism {
	case "plain":
		return []amqp.ConnOption{amqp.ConnSASLPlain(user, pass)}, nil
	case "none":
		return nil, nil
	}
	return nil, ErrSASLMechanismNotSupported(mechanism)
}

func saslFieldSpec() *service.ConfigField {
	return service.NewObjectField(saslField,
		service.NewStringAnnotatedEnumField(saslMechField, map[string]string{
			"none":  "No SASL based authentication.",
			"plain": "Plain text SASL authentication.",
		}).Description("The SASL authentication mechanism to use.").
			Default("none"),
		service.NewStringField(saslUserField).
			Description("A SASL plain text username. It is recommended that you use environment variables to populate this field.").
			Default("").
			Example("${USER}"),
		service.NewStringField(saslPassField).
			Description("A SASL plain text password. It is recommended that you use environment variables to populate this field.").
			Default("").
			Example("${PASSWORD}").
			Secret(),
	).Description("Enables SASL authentication.").Advanced().Optional()
}
