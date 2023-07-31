package amqp09

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
	bindingsDeclareField         = "bindings_declare"
	bindingsDeclareExchangeField = "exchange"
	bindingsDeclareKeyField      = "key"
	consumerTagField             = "consumer_tag"
	autoAckField                 = "auto_ack"
	nackRejectPattensField       = "nack_reject_patterns"
	prefetchCountField           = "prefetch_count"
	prefetchSizeField            = "prefetch_size"

	// Output
	exchangeField               = "exchange"
	exchangeDeclareField        = "exchange_declare"
	exchangeDeclareEnabledField = "enabled"
	exchangeDeclareTypeField    = "type"
	exchangeDeclareDurableField = "durable"
	keyField                    = "key"
	typeField                   = "type"
	contentTypeField            = "content_type"
	contentEncodingField        = "content_encoding"
	metadataFilterField         = "metadata"
	priorityField               = "priority"
	persistentField             = "persistent"
	mandatoryField              = "mandatory"
	immediateField              = "immediate"
	timeoutField                = "timeout"
	correlationIDField          = "correlation_id"
	replyToField                = "reply_to"
	expirationField             = "expiration"
	messageIDField              = "message_id"
	userIDField                 = "user_id"
	appIDField                  = "app_id"
)
