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

package ockam

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Config fields shared by the ockam_kafka input and output.

func seedBrokersField() *service.ConfigField {
	return service.NewStringListField("seed_brokers").Optional().
		Description("The address of the Kafka broker that the Ockam node's Kafka Outlet connects to. This field is only used, and is then required, when `route_to_kafka_outlet` is set to `self`. Only one address is used: the first list item, up to its first comma. If the list contains more than one item, a warning is logged.").
		ShortDescription("The Kafka broker address for the Ockam Kafka Outlet. Only used when `route_to_kafka_outlet` is `self`.").
		Example([]string{"localhost:9092"})
}

func disableContentEncryptionField() *service.ConfigField {
	return service.NewBoolField("disable_content_encryption").
		Description(`Disables encryption of Kafka message payloads.

If this value is set to ` + "`true`" + `:

* Only message payloads are unencrypted. This setting does not disable TLS or any other transport-layer encryption that may also be enabled.
* All other ` + "`ockam_kafka`" + ` inputs and outputs that use the same topic must also set this field to ` + "`true`" + `.`).
		Default(false)
}

func enrollmentTicketField() *service.ConfigField {
	return service.NewStringField("enrollment_ticket").
		Description(`The path to a file or a URL where the enrollment ticket value is stored, or an inline hex-encoded value of the enrollment ticket (optional).

You can generate a new ticket using the https://command.ockam.io/manual/ockam-project-ticket.html[` + "`ockam project ticket`" + ` command^].`).
		Optional()
}

func identityNameField() *service.ConfigField {
	return service.NewStringField("identity_name").
		Description("The name of the https://command.ockam.io/manual/ockam-identity.html[Ockam identity^] to use (optional). If an identity with this name does not exist, it is created. If this value is not provided, the default Ockam identity is used, and it is created if it does not exist.").
		Optional()
}

func allowField() *service.ConfigField {
	return service.NewStringField("allow").
		Description(`Use in conjunction with the ` + "`route_to_kafka_outlet`" + ` field to specify an access control policy for the Kafka Portal Outlet.

For example, setting this value to ` + "`kafka_us_east`" + ` forces the Kafka Outlet to present an Ockam credential, which confirms that the Outlet has the attribute ` + "`kafka_us_east=true`" + `.`).
		Default("self")
}

func routeToKafkaOutletField() *service.ConfigField {
	return service.NewStringField("route_to_kafka_outlet").
		Description("The route to reach the Kafka Portal Outlet of your Ockam portal. For example, `/project/default`.").
		Default("self")
}

func encryptedFieldsField() *service.ConfigField {
	return service.NewStringListField("encrypted_fields").
		Description("The fields to encrypt in the Kafka messages when the record is a valid JSON map. By default, the whole record is encrypted.").
		ShortDescription("Fields to encrypt within JSON records. The whole record is encrypted by default.").
		Default([]string{})
}
