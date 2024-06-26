// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package amqp1

import (
	"fmt"

	"github.com/Azure/go-amqp"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Shared
	urlField      = "url"
	urlsField     = "urls"
	tlsField      = "tls"
	saslField     = "sasl"
	saslMechField = "mechanism"
	saslUserField = "user"
	saslPassField = "password"

	// Input
	sourceAddrField       = "source_address"
	azureRenewLockField   = "azure_renew_lock"
	getMessageHeaderField = "read_header"
	creditField           = "credit"
	sourceCapsField       = "source_capabilities"

	// Output
	targetAddrField  = "target_address"
	appPropsMapField = "application_properties_map"
	metaFilterField  = "metadata"
	persistentField  = "persistent"
	targetCapsField  = "target_capabilities"
	messagePropsTo   = "message_properties_to"
)

// ErrSASLMechanismNotSupported is returned if a SASL mechanism was not recognised.
type ErrSASLMechanismNotSupported string

// Error implements the standard error interface.
func (e ErrSASLMechanismNotSupported) Error() string {
	return fmt.Sprintf("SASL mechanism %v was not recognised", string(e))
}

func saslOptFnsFromParsed(conf *service.ParsedConfig, opts *amqp.ConnOptions) error {
	if !conf.Contains(saslField) {
		return nil
	}

	conf = conf.Namespace(saslField)

	mechanism, err := conf.FieldString(saslMechField)
	if err != nil {
		return err
	}

	user, err := conf.FieldString(saslUserField)
	if err != nil {
		return err
	}

	pass, err := conf.FieldString(saslPassField)
	if err != nil {
		return err
	}

	switch mechanism {
	case "plain":
		opts.SASLType = amqp.SASLTypePlain(user, pass)
	case "anonymous":
		opts.SASLType = amqp.SASLTypeAnonymous()
	case "none":
	default:
		return ErrSASLMechanismNotSupported(mechanism)
	}
	return nil
}

func saslFieldSpec() *service.ConfigField {
	return service.NewObjectField(saslField,
		service.NewStringAnnotatedEnumField(saslMechField, map[string]string{
			"none":      "No SASL based authentication.",
			"plain":     "Plain text SASL authentication.",
			"anonymous": "Anonymous SASL authentication.",
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
