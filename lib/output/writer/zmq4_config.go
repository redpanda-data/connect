package writer

//------------------------------------------------------------------------------

// ZMQ4Config contains configuration fields for the ZMQ4 output type.
type ZMQ4Config struct {
	URLs          []string `json:"urls" yaml:"urls"`
	Bind          bool     `json:"bind" yaml:"bind"`
	SocketType    string   `json:"socket_type" yaml:"socket_type"`
	HighWaterMark int      `json:"high_water_mark" yaml:"high_water_mark"`
	PollTimeout   string   `json:"poll_timeout" yaml:"poll_timeout"`
}

// NewZMQ4Config creates a new ZMQ4Config with default values.
func NewZMQ4Config() *ZMQ4Config {
	return &ZMQ4Config{
		URLs:          []string{"tcp://*:5556"},
		Bind:          true,
		SocketType:    "PUSH",
		HighWaterMark: 0,
		PollTimeout:   "5s",
	}
}

//------------------------------------------------------------------------------
