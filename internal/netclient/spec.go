package netclient

import "github.com/redpanda-data/benthos/v4/public/service"

// ConfigSpec returns the config spec for TCP options
func ConfigSpec() *service.ConfigField {
	return service.NewObjectField("tcp",
		service.NewDurationField("tcp_user_timeout").
			Description("Linux-specific TCP_USER_TIMEOUT defines how long to wait for acknowledgment of transmitted data on an established conenction before killing the connection. Only applies to connections in ESTABLISHED state, where the network connection might be stalled and has not sent any acknowledgement back. This allows more fine grained controll on the application level as oppsed to the system-wide kernel setting, tcp_retries2 setting. Set to 0 (default) to disable.").
			Default("0s"),
		service.NewDurationField("keep_alive").
			Description("The TCP keep-alive period for the connection. If not set, this will be defaulted to 3 * tcp_user_timeout. If tcp_user_timeout is not set, this field has no effect. If setting TCP_USER_TIME, then the KEEP_ALIVE value MUST be a greater than the TCP_USER_TIMEOUT").
			Optional(),
	).
		Description("TCP socket configuration options").
		Optional()
}

// ParseConfig parses TCP config.
func ParseConfig(pConf *service.ParsedConfig) (Config, error) {
	cfg := Config{}

	if !pConf.Contains("tcp") {
		return cfg, nil
	}

	var err error
	cfg.TCPUserTimeout, err = pConf.FieldDuration("tcp", "tcp_user_timeout")
	if err != nil {
		return cfg, err
	}

	if pConf.Contains("tcp", "keep_alive") {
		cfg.KeepAlive, err = pConf.FieldDuration("tcp", "keep_alive")
		if err != nil {
			return cfg, err
		}
	}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}
