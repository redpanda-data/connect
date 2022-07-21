package input

// NanomsgConfig contains configuration fields for the nanomsg input type.
type NanomsgConfig struct {
	URLs        []string `json:"urls" yaml:"urls"`
	Bind        bool     `json:"bind" yaml:"bind"`
	SocketType  string   `json:"socket_type" yaml:"socket_type"`
	SubFilters  []string `json:"sub_filters" yaml:"sub_filters"`
	PollTimeout string   `json:"poll_timeout" yaml:"poll_timeout"`
}

// NewNanomsgConfig creates a new NanomsgConfig with default values.
func NewNanomsgConfig() NanomsgConfig {
	return NanomsgConfig{
		URLs:        []string{},
		Bind:        true,
		SocketType:  "PULL",
		SubFilters:  []string{},
		PollTimeout: "5s",
	}
}
