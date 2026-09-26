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

package amqp09

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Shared
	urlsField = "urls"
	tlsField  = "tls"

	// Input
	queueField                   = "queue"
	queueDeclareField            = "queue_declare"
	queueDeclareEnabledField     = "enabled"
	queueDeclareDurableField     = "durable"
	queueDeclareAutoDeleteField  = "auto_delete"
	queueDeclareArgumentsField   = "arguments"
	bindingsDeclareField         = "bindings_declare"
	bindingsDeclareExchangeField = "exchange"
	bindingsDeclareKeyField      = "key"
	consumerTagField             = "consumer_tag"
	autoAckField                 = "auto_ack"
	nackRejectPattensField       = "nack_reject_patterns"
	prefetchCountField           = "prefetch_count"
	prefetchSizeField            = "prefetch_size"

	// Output
	exchangeField                 = "exchange"
	exchangeDeclareField          = "exchange_declare"
	exchangeDeclareEnabledField   = "enabled"
	exchangeDeclareTypeField      = "type"
	exchangeDeclareDurableField   = "durable"
	exchangeDeclareArgumentsField = "arguments"
	keyField                      = "key"
	typeField                     = "type"
	contentTypeField              = "content_type"
	contentEncodingField          = "content_encoding"
	metadataFilterField           = "metadata"
	priorityField                 = "priority"
	persistentField               = "persistent"
	mandatoryField                = "mandatory"
	immediateField                = "immediate"
	timeoutField                  = "timeout"
	correlationIDField            = "correlation_id"
	replyToField                  = "reply_to"
	expirationField               = "expiration"
	messageIDField                = "message_id"
	userIDField                   = "user_id"
	appIDField                    = "app_id"
)

// urlsFieldSpec returns the urls field shared by the input and output.
func urlsFieldSpec() *service.ConfigField {
	return service.NewURLListField(urlsField).
		Description(`A list of URLs to connect to. Each URL in the list is tried in order until a connection is established, and that URL is then used until the connection is closed.

If an item in the list contains commas, it is split into multiple URLs.`).
		ShortDescription("URLs to connect to. The first to connect successfully is used until the connection closes.").
		Example([]string{"amqp://guest:guest@127.0.0.1:5672/"}).
		Example([]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"}).
		Example([]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"}).
		Version("3.58.0")
}
