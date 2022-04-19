package benthos

#Config: {
	http?: {
		enabled?:         bool
		address?:         string
		root_path?:       string
		debug_endpoints?: bool
		cert_file?:       string
		key_file?:        string
		cors?: {
			enabled?: bool
			allowed_origins?: [...string]
		}
	}
	input?:  null | #Input
	buffer?: null | #Buffer
	pipeline?: {
		threads?: int
		processors?: [...null | #Processor]
	}
	output?: null | #Output
	input_resources?: [...null | #Input]
	processor_resources?: [...null | #Processor]
	output_resources?: [...null | #Output]
	cache_resources?: [...null | #Cache]
	rate_limit_resources?: [...null | #RateLimit]
	logger?: {
		level?:         string
		format?:        string
		add_timestamp?: bool
		static_fields?: {
			[string]: string
		}
	}
	metrics?:          null | #Metric
	tracer?:           null | #Tracer
	shutdown_timeout?: string
	tests: [...{
		name: string
		environment?: {
			[string]: string
		}
		target_processors?: string
		target_mapping?:    string
		mocks?: {
			[string]: _
		}
		input_batch?: [...{
			content?:      string
			json_content?: _
			file_content?: string
			metadata?: {
				[string]: string
			}
		}]
		output_batches?: [[...{
			content?: string
			metadata?: {
				[string]: string
			}
			bloblang?:        string
			content_equals?:  string
			content_matches?: string
			metadata_equals?: {
				[string]: string
			}
			file_equals?:      string
			file_json_equals?: string
			json_equals?:      _
			json_contains?:    string
		}]]
	}]
}
#AllInputs: {
	amqp_0_9: {
		urls?: [...string]
		queue?: string
		queue_declare?: {
			enabled?: bool
			durable?: bool
		}
		bindings_declare?: [...{
			exchange?: string
			key?:      string
		}]
		consumer_tag?: string
		auto_ack?:     bool
		nack_reject_patterns?: [...string]
		prefetch_count?: int
		prefetch_size?:  int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
	}
	amqp_1: {
		url?:              string
		source_address?:   string
		azure_renew_lock?: bool
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		sasl?: {
			mechanism?: string
			user?:      string
			password?:  string
		}
	}
	aws_kinesis: {
		streams?: [...string]
		dynamodb?: {
			table?:                string
			create?:               bool
			billing_mode?:         string
			read_capacity_units?:  int
			write_capacity_units?: int
		}
		checkpoint_limit?:  int
		commit_period?:     string
		rebalance_period?:  string
		lease_period?:      string
		start_from_oldest?: bool
		region?:            string
		endpoint?:          string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	aws_s3: {
		bucket?:   string
		prefix?:   string
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		force_path_style_urls?: bool
		delete_objects?:        bool
		codec?:                 string
		sqs?: {
			url?:           string
			endpoint?:      string
			key_path?:      string
			bucket_path?:   string
			envelope_path?: string
			delay_period?:  string
			max_messages?:  int
		}
	}
	aws_sqs: {
		url?:                    string
		delete_message?:         bool
		reset_visibility?:       bool
		max_number_of_messages?: int
		region?:                 string
		endpoint?:               string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	azure_blob_storage: {
		storage_account?:           string
		storage_access_key?:        string
		storage_sas_token?:         string
		storage_connection_string?: string
		container?:                 string
		prefix?:                    string
		codec?:                     string
		delete_objects?:            bool
	}
	azure_queue_storage: {
		storage_account?:            string
		storage_access_key?:         string
		storage_sas_token?:          string
		storage_connection_string?:  string
		queue_name?:                 string
		dequeue_visibility_timeout?: string
		max_in_flight?:              int
		track_properties?:           bool
	}
	broker: {
		copies?: int
		inputs?: [...null | #Input]
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	csv: {
		paths?: [...string]
		parse_header_row?: bool
		delimiter?:        string
		batch_count?:      int
	}
	discord: {
		channel_id:   string
		bot_token:    string
		poll_period?: string
		limit?:       int
		cache:        string
		cache_key?:   string
		rate_limit?:  string
	}
	dynamic: {
		inputs?: {
			[string]: null | #Input
		}
		prefix?: string
	}
	file: {
		paths?: [...string]
		codec?:            string
		max_buffer?:       int
		delete_on_finish?: bool
	}
	gcp_bigquery_select: {
		project: string
		table:   string
		columns: [...string]
		where?: string
		job_labels?: {
			[string]: string
		}
		args_mapping?: string
		prefix?:       string
		suffix?:       string
	}
	gcp_cloud_storage: {
		bucket?:         string
		prefix?:         string
		codec?:          string
		delete_objects?: bool
	}
	gcp_pubsub: {
		project?:                  string
		subscription?:             string
		sync?:                     bool
		max_outstanding_messages?: int
		max_outstanding_bytes?:    int
	}
	generate: {
		mapping?:  string
		interval?: string
		count?:    int
	}
	hdfs: {
		hosts?: [...string]
		user?:      string
		directory?: string
	}
	http_client: {
		url?:  string
		verb?: string
		headers?: {
			[string]: string
		}
		metadata?: {
			include_prefixes?: [...string]
			include_patterns?: [...string]
		}
		oauth?: {
			enabled?:             bool
			consumer_key?:        string
			consumer_secret?:     string
			access_token?:        string
			access_token_secret?: string
		}
		oauth2?: {
			enabled?:       bool
			client_key?:    string
			client_secret?: string
			token_url?:     string
			scopes?: [...string]
		}
		jwt?: {
			enabled?:          bool
			private_key_file?: string
			signing_method?:   string
			claims?: {
				[string]: _
			}
			headers?: {
				[string]: _
			}
		}
		basic_auth?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		extract_headers?: {
			include_prefixes?: [...string]
			include_patterns?: [...string]
		}
		rate_limit?:        string
		timeout?:           string
		retry_period?:      string
		max_retry_backoff?: string
		retries?:           int
		backoff_on?: [...int]
		drop_on?: [...int]
		successful_on?: [...int]
		proxy_url?:         string
		payload?:           string
		drop_empty_bodies?: bool
		stream?: {
			enabled?:    bool
			reconnect?:  bool
			codec?:      string
			max_buffer?: int
		}
	}
	http_server: {
		address?:               string
		path?:                  string
		ws_path?:               string
		ws_welcome_message?:    string
		ws_rate_limit_message?: string
		allowed_verbs?: [...string]
		timeout?:    string
		rate_limit?: string
		cert_file?:  string
		key_file?:   string
		cors?: {
			enabled?: bool
			allowed_origins?: [...string]
		}
		sync_response?: {
			status?: string
			headers?: {
				[string]: string
			}
			metadata_headers?: {
				include_prefixes?: [...string]
				include_patterns?: [...string]
			}
		}
	}
	inproc: string
	kafka: {
		addresses?: [...string]
		topics?: [...string]
		target_version?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		sasl?: {
			mechanism?:    string
			user?:         string
			password?:     string
			access_token?: string
			token_cache?:  string
			token_key?:    string
		}
		consumer_group?:        string
		client_id?:             string
		rack_id?:               string
		start_from_oldest?:     bool
		checkpoint_limit?:      int
		commit_period?:         string
		max_processing_period?: string
		extract_tracing_map?:   string
		group?: {
			session_timeout?:    string
			heartbeat_interval?: string
			rebalance_timeout?:  string
		}
		fetch_buffer_cap?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	kafka_franz: {
		seed_brokers: [...string]
		topics: [...string]
		regexp_topics?:    bool
		consumer_group:    string
		checkpoint_limit?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		sasl?: [...{
			mechanism: string
			username?: string
			password?: string
			token?:    string
			extensions?: {
				[string]: string
			}
		}]
	}
	mongodb: {
		url:        string
		database:   string
		collection: string
		username?:  string
		password?:  string
		query:      string
	}
	mqtt: {
		urls?: [...string]
		topics?: [...string]
		client_id?:                string
		dynamic_client_id_suffix?: string
		qos?:                      int
		clean_session?:            bool
		will?: {
			enabled?:  bool
			qos?:      int
			retained?: bool
			topic?:    string
			payload?:  string
		}
		connect_timeout?: string
		user?:            string
		password?:        string
		keepalive?:       int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
	}
	nanomsg: {
		urls?: [...string]
		bind?:        bool
		socket_type?: string
		sub_filters?: [...string]
		poll_timeout?: string
	}
	nats: {
		urls?: [...string]
		queue?:          string
		subject?:        string
		prefetch_count?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		auth?: {
			nkey_file?:             string
			user_credentials_file?: string
		}
	}
	nats_jetstream: {
		urls: [...string]
		queue?:           string
		subject?:         string
		durable?:         string
		stream?:          string
		bind?:            bool
		deliver?:         string
		ack_wait?:        string
		max_ack_pending?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		auth?: {
			nkey_file?:             string
			user_credentials_file?: string
		}
	}
	nats_stream: {
		urls?: [...string]
		cluster_id?:           string
		client_id?:            string
		queue?:                string
		subject?:              string
		durable_name?:         string
		unsubscribe_on_close?: bool
		start_from_oldest?:    bool
		max_inflight?:         int
		ack_wait?:             string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		auth?: {
			nkey_file?:             string
			user_credentials_file?: string
		}
	}
	nsq: {
		nsqd_tcp_addresses?: [...string]
		lookupd_http_addresses?: [...string]
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		topic?:         string
		channel?:       string
		user_agent?:    string
		max_in_flight?: int
	}
	read_until: {
		input?:         null | #Input
		check?:         string
		restart_input?: bool
	}
	redis_list: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		key?:     string
		timeout?: string
	}
	redis_pubsub: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		channels?: [...string]
		use_patterns?: bool
	}
	redis_streams: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		body_key?: string
		streams?: [...string]
		limit?:             int
		client_id?:         string
		consumer_group?:    string
		create_streams?:    bool
		start_from_oldest?: bool
		commit_period?:     string
		timeout?:           string
	}
	resource: string
	sequence: {
		sharded_join?: {
			type?:           string
			id_path?:        string
			iterations?:     int
			merge_strategy?: string
		}
		inputs?: [...null | #Input]
	}
	sftp: {
		address?: string
		credentials?: {
			username?:         string
			password?:         string
			private_key_file?: string
			private_key_pass?: string
		}
		paths?: [...string]
		codec?:            string
		delete_on_finish?: bool
		max_buffer?:       int
		watcher?: {
			enabled?:       bool
			minimum_age?:   string
			poll_interval?: string
			cache?:         string
		}
	}
	socket: {
		network?:    string
		address?:    string
		codec?:      string
		max_buffer?: int
	}
	socket_server: {
		network?:    string
		address?:    string
		codec?:      string
		max_buffer?: int
	}
	sql_select: {
		driver: string
		dsn:    string
		table:  string
		columns: [...string]
		where?:              string
		args_mapping?:       string
		prefix?:             string
		suffix?:             string
		conn_max_idle_time?: string
		conn_max_life_time?: string
		conn_max_idle?:      int
		conn_max_open?:      int
	}
	stdin: {
		codec?:      string
		max_buffer?: int
	}
	subprocess: {
		name?: string
		args?: [...string]
		codec?:           string
		restart_on_exit?: bool
		max_buffer?:      int
	}
	twitter_search: {
		query: string
		tweet_fields?: [...string]
		poll_period?:     string
		backfill_period?: string
		cache:            string
		cache_key?:       string
		rate_limit?:      string
		api_key:          string
		api_secret:       string
	}
	websocket: {
		url?:          string
		open_message?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		oauth?: {
			enabled?:             bool
			consumer_key?:        string
			consumer_secret?:     string
			access_token?:        string
			access_token_secret?: string
		}
		basic_auth?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		jwt?: {
			enabled?:          bool
			private_key_file?: string
			signing_method?:   string
			claims?: {
				[string]: _
			}
			headers?: {
				[string]: _
			}
		}
	}
}
#Input: or([ for name, config in #AllInputs {
	(name): config
}])
#Input: #Input & {
	processors?: [...#Processor]
	label?: string
}
#AllOutputs: {
	amqp_0_9: {
		urls?: [...string]
		exchange?: string
		exchange_declare?: {
			enabled?: bool
			type?:    string
			durable?: bool
		}
		key?:              string
		type?:             string
		content_type?:     string
		content_encoding?: string
		metadata?: {
			exclude_prefixes?: [...string]
		}
		priority?:      string
		max_in_flight?: int
		persistent?:    bool
		mandatory?:     bool
		immediate?:     bool
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
	}
	amqp_1: {
		url?:            string
		target_address?: string
		max_in_flight?:  int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		sasl?: {
			mechanism?: string
			user?:      string
			password?:  string
		}
		metadata?: {
			exclude_prefixes?: [...string]
		}
	}
	aws_dynamodb: {
		table?: string
		string_columns?: {
			[string]: string
		}
		json_map_columns?: {
			[string]: string
		}
		ttl?:           string
		ttl_key?:       string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	aws_kinesis: {
		stream?:        string
		partition_key?: string
		hash_key?:      string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	aws_kinesis_firehose: {
		stream?:        string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	aws_s3: {
		bucket?: string
		path?:   string
		tags?: {
			[string]: string
		}
		content_type?:              string
		content_encoding?:          string
		cache_control?:             string
		content_disposition?:       string
		content_language?:          string
		website_redirect_location?: string
		metadata?: {
			exclude_prefixes?: [...string]
		}
		storage_class?:          string
		kms_key_id?:             string
		server_side_encryption?: string
		force_path_style_urls?:  bool
		max_in_flight?:          int
		timeout?:                string
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	aws_sns: {
		topic_arn?:                string
		message_group_id?:         string
		message_deduplication_id?: string
		max_in_flight?:            int
		metadata?: {
			exclude_prefixes?: [...string]
		}
		timeout?:  string
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	aws_sqs: {
		url?:                      string
		message_group_id?:         string
		message_deduplication_id?: string
		max_in_flight?:            int
		metadata?: {
			exclude_prefixes?: [...string]
		}
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	azure_blob_storage: {
		storage_account?:           string
		storage_access_key?:        string
		storage_sas_token?:         string
		storage_connection_string?: string
		public_access_level?:       string
		container?:                 string
		path?:                      string
		blob_type?:                 string
		max_in_flight?:             int
	}
	azure_queue_storage: {
		storage_account?:           string
		storage_access_key?:        string
		storage_connection_string?: string
		queue_name?:                string
		ttl?:                       string
		max_in_flight?:             int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	azure_table_storage: {
		storage_account?:           string
		storage_access_key?:        string
		storage_connection_string?: string
		table_name?:                string
		partition_key?:             string
		row_key?:                   string
		properties?: {
			[string]: string
		}
		insert_type?:   string
		max_in_flight?: int
		timeout?:       string
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	broker: {
		copies?:  int
		pattern?: string
		outputs?: [...null | #Output]
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	cache: {
		target?:        string
		key?:           string
		ttl?:           string
		max_in_flight?: int
	}
	cassandra: {
		addresses?: [...string]
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		password_authenticator?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		disable_initial_host_lookup?: bool
		query?:                       string
		args_mapping?:                string
		consistency?:                 string
		max_retries?:                 int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
		timeout?:       string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	discord: {
		channel_id:  string
		bot_token:   string
		rate_limit?: string
	}
	drop: {}
	drop_on: {
		error?:         bool
		back_pressure?: string
		output?:        null | #Output
	}
	dynamic: {
		outputs?: {
			[string]: null | #Output
		}
		prefix?: string
	}
	elasticsearch: {
		urls?: [...string]
		index?:       string
		action?:      string
		pipeline?:    string
		id?:          string
		type?:        string
		routing?:     string
		sniff?:       bool
		healthcheck?: bool
		timeout?:     string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		max_in_flight?: int
		max_retries?:   int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
		basic_auth?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		aws?: {
			enabled?:  bool
			region?:   string
			endpoint?: string
			credentials?: {
				profile?:          string
				id?:               string
				secret?:           string
				token?:            string
				role?:             string
				role_external_id?: string
			}
		}
		gzip_compression?: bool
	}
	fallback: [...null | #Output]
	file: {
		path?:  string
		codec?: string
	}
	gcp_bigquery: {
		project?:               string
		dataset:                string
		table:                  string
		format?:                string
		max_in_flight?:         int
		write_disposition?:     string
		create_disposition?:    string
		ignore_unknown_values?: bool
		max_bad_records?:       int
		auto_detect?:           bool
		csv?: {
			header?: [...string]
			field_delimiter?:       string
			allow_jagged_rows?:     bool
			allow_quoted_newlines?: bool
			encoding?:              string
			skip_leading_rows?:     int
		}
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	gcp_cloud_storage: {
		bucket?:           string
		path?:             string
		content_type?:     string
		collision_mode?:   string
		content_encoding?: string
		chunk_size?:       int
		max_in_flight?:    int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	gcp_pubsub: {
		project?:         string
		topic?:           string
		max_in_flight?:   int
		publish_timeout?: string
		ordering_key?:    string
		metadata?: {
			exclude_prefixes?: [...string]
		}
	}
	hdfs: {
		hosts?: [...string]
		user?:          string
		directory?:     string
		path?:          string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	http_client: {
		url?:  string
		verb?: string
		headers?: {
			[string]: string
		}
		metadata?: {
			include_prefixes?: [...string]
			include_patterns?: [...string]
		}
		oauth?: {
			enabled?:             bool
			consumer_key?:        string
			consumer_secret?:     string
			access_token?:        string
			access_token_secret?: string
		}
		oauth2?: {
			enabled?:       bool
			client_key?:    string
			client_secret?: string
			token_url?:     string
			scopes?: [...string]
		}
		jwt?: {
			enabled?:          bool
			private_key_file?: string
			signing_method?:   string
			claims?: {
				[string]: _
			}
			headers?: {
				[string]: _
			}
		}
		basic_auth?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		extract_headers?: {
			include_prefixes?: [...string]
			include_patterns?: [...string]
		}
		rate_limit?:        string
		timeout?:           string
		retry_period?:      string
		max_retry_backoff?: string
		retries?:           int
		backoff_on?: [...int]
		drop_on?: [...int]
		successful_on?: [...int]
		proxy_url?:          string
		batch_as_multipart?: bool
		propagate_response?: bool
		max_in_flight?:      int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		multipart?: [...{
			content_type?:        string
			content_disposition?: string
			body?:                string
		}]
	}
	http_server: {
		address?:     string
		path?:        string
		stream_path?: string
		ws_path?:     string
		allowed_verbs?: [...string]
		timeout?:   string
		cert_file?: string
		key_file?:  string
		cors?: {
			enabled?: bool
			allowed_origins?: [...string]
		}
	}
	inproc: string
	kafka: {
		addresses?: [...string]
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		sasl?: {
			mechanism?:    string
			user?:         string
			password?:     string
			access_token?: string
			token_cache?:  string
			token_key?:    string
		}
		topic?:          string
		client_id?:      string
		target_version?: string
		rack_id?:        string
		key?:            string
		partitioner?:    string
		partition?:      string
		compression?:    string
		static_headers?: {
			[string]: string
		}
		metadata?: {
			exclude_prefixes?: [...string]
		}
		inject_tracing_map?: string
		max_in_flight?:      int
		ack_replicas?:       bool
		max_msg_bytes?:      int
		timeout?:            string
		retry_as_batch?:     bool
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	kafka_franz: {
		seed_brokers: [...string]
		topic:        string
		key?:         string
		partitioner?: string
		metadata?: {
			include_prefixes: [...string]
			include_patterns: [...string]
		}
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		max_message_bytes?: string
		compression?:       string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		sasl?: [...{
			mechanism: string
			username?: string
			password?: string
			token?:    string
			extensions?: {
				[string]: string
			}
		}]
	}
	mongodb: {
		url?:        string
		database?:   string
		username?:   string
		password?:   string
		operation?:  string
		collection?: string
		write_concern?: {
			w?:         string
			j?:         bool
			w_timeout?: string
		}
		document_map?:  string
		filter_map?:    string
		hint_map?:      string
		upsert?:        bool
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	mqtt: {
		urls?: [...string]
		topic?:                    string
		client_id?:                string
		dynamic_client_id_suffix?: string
		qos?:                      int
		connect_timeout?:          string
		write_timeout?:            string
		retained?:                 bool
		retained_interpolated?:    string
		will?: {
			enabled?:  bool
			qos?:      int
			retained?: bool
			topic?:    string
			payload?:  string
		}
		user?:      string
		password?:  string
		keepalive?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		max_in_flight?: int
	}
	nanomsg: {
		urls?: [...string]
		bind?:          bool
		socket_type?:   string
		poll_timeout?:  string
		max_in_flight?: int
	}
	nats: {
		urls?: [...string]
		subject?: string
		headers?: {
			[string]: string
		}
		max_in_flight?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		auth?: {
			nkey_file?:             string
			user_credentials_file?: string
		}
	}
	nats_jetstream: {
		urls: [...string]
		subject:        string
		max_in_flight?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		auth?: {
			nkey_file?:             string
			user_credentials_file?: string
		}
	}
	nats_stream: {
		urls?: [...string]
		cluster_id?:    string
		subject?:       string
		client_id?:     string
		max_in_flight?: int
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		auth?: {
			nkey_file?:             string
			user_credentials_file?: string
		}
	}
	nsq: {
		nsqd_tcp_address?: string
		topic?:            string
		user_agent?:       string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		max_in_flight?: int
	}
	redis_hash: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		key?:              string
		walk_metadata?:    bool
		walk_json_object?: bool
		fields?: {
			[string]: string
		}
		max_in_flight?: int
	}
	redis_list: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		key?:           string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	redis_pubsub: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		channel?:       string
		max_in_flight?: int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	redis_streams: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		stream?:        string
		body_key?:      string
		max_length?:    int
		max_in_flight?: int
		metadata?: {
			exclude_prefixes?: [...string]
		}
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	reject:   string
	resource: string
	retry: {
		max_retries?: int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
		output: null | #Output
	}
	sftp: {
		address?: string
		path?:    string
		codec?:   string
		credentials?: {
			username?:         string
			password?:         string
			private_key_file?: string
			private_key_pass?: string
		}
		max_in_flight?: int
	}
	snowflake_put: {
		account:                  string
		region?:                  string
		cloud?:                   string
		user:                     string
		password?:                string
		private_key_file?:        string
		private_key_pass?:        string
		role:                     string
		database:                 string
		warehouse:                string
		schema:                   string
		stage:                    string
		path:                     string
		upload_parallel_threads?: int
		compression?:             string
		snowpipe?:                string
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
		max_in_flight?: int
	}
	socket: {
		network?: string
		address?: string
		codec?:   string
	}
	sql: {
		driver:           string
		data_source_name: string
		query:            string
		args_mapping?:    string
		max_in_flight?:   int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	sql_insert: {
		driver: string
		dsn:    string
		table:  string
		columns: [...string]
		args_mapping:        string
		prefix?:             string
		suffix?:             string
		max_in_flight?:      int
		conn_max_idle_time?: string
		conn_max_life_time?: string
		conn_max_idle?:      int
		conn_max_open?:      int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	sql_raw: {
		driver:              string
		dsn:                 string
		query:               string
		args_mapping?:       string
		max_in_flight?:      int
		conn_max_idle_time?: string
		conn_max_life_time?: string
		conn_max_idle?:      int
		conn_max_open?:      int
		batching?: {
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	stdout: {
		codec?: string
	}
	subprocess: {
		name?: string
		args?: [...string]
		codec?: string
	}
	switch: {
		retry_until_success?: bool
		strict_mode?:         bool
		cases?: [...{
			check?:    string
			output?:   null | #Output
			continue?: bool
		}]
	}
	sync_response: {}
	websocket: {
		url?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		oauth?: {
			enabled?:             bool
			consumer_key?:        string
			consumer_secret?:     string
			access_token?:        string
			access_token_secret?: string
		}
		basic_auth?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		jwt?: {
			enabled?:          bool
			private_key_file?: string
			signing_method?:   string
			claims?: {
				[string]: _
			}
			headers?: {
				[string]: _
			}
		}
	}
}
#Output: or([ for name, config in #AllOutputs {
	(name): config
}])
#Output: #Output & {
	processors?: [...#Processor]
	label?: string
}
#AllProcessors: {
	archive: {
		format?: string
		path?:   string
	}
	avro: {
		operator?:    string
		encoding?:    string
		schema?:      string
		schema_path?: string
	}
	awk: {
		codec?:   string
		program?: string
	}
	aws_dynamodb_partiql: {
		query:                 string
		unsafe_dynamic_query?: bool
		args_mapping?:         string
		region?:               string
		endpoint?:             string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	aws_lambda: {
		parallel?:   bool
		function:    string
		rate_limit?: string
		region?:     string
		endpoint?:   string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
		timeout?: string
		retries?: int
	}
	bloblang: string
	bounds_check: {
		max_part_size?: int
		min_part_size?: int
		max_parts?:     int
		min_parts?:     int
	}
	branch: {
		request_map?: string
		processors?: [...null | #Processor]
		result_map?: string
	}
	cache: {
		resource?: string
		operator?: string
		key?:      string
		value?:    string
		ttl?:      string
	}
	catch: [...null | #Processor]
	compress: {
		algorithm?: string
		level?:     int
	}
	decompress: {
		algorithm?: string
	}
	dedupe: {
		cache?:       string
		key?:         string
		drop_on_err?: bool
	}
	for_each: [...null | #Processor]
	gcp_bigquery_select: {
		project: string
		table:   string
		columns: [...string]
		where?: string
		job_labels?: {
			[string]: string
		}
		args_mapping?: string
		prefix?:       string
		suffix?:       string
	}
	grok: {
		expressions?: [...string]
		pattern_definitions?: {
			[string]: string
		}
		pattern_paths?: [...string]
		named_captures_only?:  bool
		use_default_patterns?: bool
		remove_empty_values?:  bool
	}
	group_by: {
		check?: string
		processors?: [...null | #Processor]
	}
	group_by_value: {
		value?: string
	}
	http: {
		url?:  string
		verb?: string
		headers?: {
			[string]: string
		}
		metadata?: {
			include_prefixes?: [...string]
			include_patterns?: [...string]
		}
		oauth?: {
			enabled?:             bool
			consumer_key?:        string
			consumer_secret?:     string
			access_token?:        string
			access_token_secret?: string
		}
		oauth2?: {
			enabled?:       bool
			client_key?:    string
			client_secret?: string
			token_url?:     string
			scopes?: [...string]
		}
		jwt?: {
			enabled?:          bool
			private_key_file?: string
			signing_method?:   string
			claims?: {
				[string]: _
			}
			headers?: {
				[string]: _
			}
		}
		basic_auth?: {
			enabled?:  bool
			username?: string
			password?: string
		}
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		extract_headers?: {
			include_prefixes?: [...string]
			include_patterns?: [...string]
		}
		rate_limit?:        string
		timeout?:           string
		retry_period?:      string
		max_retry_backoff?: string
		retries?:           int
		backoff_on?: [...int]
		drop_on?: [...int]
		successful_on?: [...int]
		proxy_url?:          string
		batch_as_multipart?: bool
		parallel?:           bool
	}
	insert_part: {
		index?:   int
		content?: string
	}
	jmespath: {
		query?: string
	}
	jq: {
		query?:      string
		raw?:        bool
		output_raw?: bool
	}
	json_schema: {
		schema?:      string
		schema_path?: string
	}
	log: {
		level?: string
		fields?: {
			[string]: string
		}
		fields_mapping?: string
		message?:        string
	}
	metric: {
		type?: string
		name?: string
		labels?: {
			[string]: string
		}
		value?: string
	}
	mongodb: {
		url?:        string
		database?:   string
		username?:   string
		password?:   string
		operation?:  string
		collection?: string
		write_concern?: {
			w?:         string
			j?:         bool
			w_timeout?: string
		}
		document_map?:      string
		filter_map?:        string
		hint_map?:          string
		upsert?:            bool
		json_marshal_mode?: string
		max_retries?:       int
		backoff?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	msgpack: {
		operator: string
	}
	noop: {}
	parallel: {
		cap?: int
		processors?: [...null | #Processor]
	}
	parquet: {
		operator:     string
		compression?: string
		schema_file?: string
		schema?:      string
	}
	parse_log: {
		format?:           string
		codec?:            string
		best_effort?:      bool
		allow_rfc3339?:    bool
		default_year?:     string
		default_timezone?: string
	}
	protobuf: {
		operator?: string
		message?:  string
		import_paths?: [...string]
	}
	rate_limit: {
		resource?: string
	}
	redis: {
		url?:    string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		operator?:     string
		key?:          string
		retries?:      int
		retry_period?: string
	}
	resource: string
	schema_registry_decode: {
		url: string
		tls?: {
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
	}
	schema_registry_encode: {
		url:             string
		subject:         string
		refresh_period?: string
		avro_raw_json?:  bool
		tls?: {
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
	}
	select_parts: {
		parts?: [...int]
	}
	sleep: {
		duration?: string
	}
	split: {
		size?:      int
		byte_size?: int
	}
	sql: {
		driver:                string
		data_source_name:      string
		query:                 string
		unsafe_dynamic_query?: bool
		args_mapping?:         string
		result_codec?:         string
	}
	sql_insert: {
		driver: string
		dsn:    string
		table:  string
		columns: [...string]
		args_mapping:        string
		prefix?:             string
		suffix?:             string
		conn_max_idle_time?: string
		conn_max_life_time?: string
		conn_max_idle?:      int
		conn_max_open?:      int
	}
	sql_raw: {
		driver:                string
		dsn:                   string
		query:                 string
		unsafe_dynamic_query?: bool
		args_mapping?:         string
		exec_only?:            bool
		conn_max_idle_time?:   string
		conn_max_life_time?:   string
		conn_max_idle?:        int
		conn_max_open?:        int
	}
	sql_select: {
		driver: string
		dsn:    string
		table:  string
		columns: [...string]
		where?:              string
		args_mapping?:       string
		prefix?:             string
		suffix?:             string
		conn_max_idle_time?: string
		conn_max_life_time?: string
		conn_max_idle?:      int
		conn_max_open?:      int
	}
	subprocess: {
		name?: string
		args?: [...string]
		max_buffer?: int
		codec_send?: string
		codec_recv?: string
	}
	switch: {
		check?: string
		processors?: [...null | #Processor]
		fallthrough?: bool
	}
	sync_response: {}
	try: [...null | #Processor]
	unarchive: {
		format?: string
	}
	while: {
		at_least_once?: bool
		max_loops?:     int
		check?:         string
		processors?: [...null | #Processor]
	}
	workflow: {
		meta_path?: string
		order?: [[...string]]
		branch_resources?: [...string]
		branches?: {
			[string]: {
				request_map?: string
				processors?: [...null | #Processor]
				result_map?: string
			}
		}
	}
	xml: {
		operator?: string
		cast?:     bool
	}
}
#Processor: or([ for name, config in #AllProcessors {
	(name): config
}])
#Processor: #Processor & {
	processors?: [...#Processor]
	label?: string
}
#AllCaches: {
	aws_dynamodb: {
		table:            string
		hash_key:         string
		data_key:         string
		consistent_read?: bool
		default_ttl?:     string
		ttl_key?:         string
		retries?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	aws_s3: {
		bucket:                 string
		content_type?:          string
		force_path_style_urls?: bool
		retries?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
		region?:   string
		endpoint?: string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	file: {
		directory: string
	}
	gcp_cloud_storage: {
		bucket: string
	}
	memcached: {
		addresses: [...string]
		prefix?:      string
		default_ttl?: string
		retries?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	memory: {
		default_ttl?:         string
		compaction_interval?: string
		init_values?: {
			[string]: string
		}
		shards?: int
	}
	mongodb: {
		url:         string
		username?:   string
		password?:   string
		database:    string
		collection:  string
		key_field:   string
		value_field: string
	}
	multilevel: [...string]
	redis: {
		url:     string
		kind?:   string
		master?: string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		prefix?:      string
		default_ttl?: string
		retries?: {
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
	ristretto: {
		default_ttl?: string
		get_retries?: {
			enabled?:          bool
			initial_interval?: string
			max_interval?:     string
			max_elapsed_time?: string
		}
	}
}
#Cache: or([ for name, config in #AllCaches {
	(name): config
}])
#AllRateLimits: {
	local: {
		count?:    int
		interval?: string
	}
}
#RateLimit: or([ for name, config in #AllRateLimits {
	(name): config
}])
#AllBuffers: {
	memory: {
		limit?: int
		batch_policy?: {
			enabled?:   bool
			count?:     int
			byte_size?: int
			period?:    string
			check?:     string
			processors?: [...null | #Processor]
		}
	}
	none: {}
	system_window: {
		timestamp_mapping?: string
		size:               string
		slide?:             string
		offset?:            string
		allowed_lateness?:  string
	}
}
#Buffer: or([ for name, config in #AllBuffers {
	(name): config
}])
#AllMetrics: {
	aws_cloudwatch: {
		namespace?:    string
		flush_period?: string
		region?:       string
		endpoint?:     string
		credentials?: {
			profile?:          string
			id?:               string
			secret?:           string
			token?:            string
			role?:             string
			role_external_id?: string
		}
	}
	influxdb: {
		url?: string
		db?:  string
		tls?: {
			enabled?:              bool
			skip_cert_verify?:     bool
			enable_renegotiation?: bool
			root_cas?:             string
			root_cas_file?:        string
			client_certs?: [...{
				cert?:      string
				key?:       string
				cert_file?: string
				key_file?:  string
			}]
		}
		username?: string
		password?: string
		include?: {
			runtime?:  string
			debug_gc?: string
		}
		interval?:      string
		ping_interval?: string
		precision?:     string
		timeout?:       string
		tags?: {
			[string]: string
		}
		retention_policy?:  string
		write_consistency?: string
	}
	json_api: {}
	logger: {
		push_interval?: string
		flush_metrics?: bool
	}
	none: {}
	prometheus: {
		use_histogram_timing?: bool
		histogram_buckets?: [...float]
		add_process_metrics?: bool
		add_go_metrics?:      bool
		push_url?:            string
		push_interval?:       string
		push_job_name?:       string
		push_basic_auth?: {
			username?: string
			password?: string
		}
		file_output_path?: string
	}
	statsd: {
		address?:      string
		flush_period?: string
		tag_format?:   string
	}
}
#Metric: or([ for name, config in #AllMetrics {
	(name): config
}])
#AllTracers: {
	jaeger: {
		agent_address?: string
		collector_url?: string
		sampler_type?:  string
		sampler_param?: float
		tags?: {
			[string]: string
		}
		flush_interval?: string
	}
	none: {}
}
#Tracer: or([ for name, config in #AllTracers {
	(name): config
}])

