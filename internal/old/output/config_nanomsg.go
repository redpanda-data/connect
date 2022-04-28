package output

// NanomsgConfig contains configuration fields for the Nanomsg output type.
type NanomsgConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	Bind        bool     `json:"bind" yaml:"bind"`
	SocketType  string   `json:"socket_type" yaml:"socket_type"`
	PollTimeout string   `json:"poll_timeout" yaml:"poll_timeout"`
	MaxInFlight int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNanomsgConfig creates a new NanomsgConfig with default values.
func NewNanomsgConfig() NanomsgConfig {
	return NanomsgConfig{
		URLs:        []string{},
		Bind:        false,
		SocketType:  "PUSH",
		PollTimeout: "5s",
		MaxInFlight: 64,
	}
}
