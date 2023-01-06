package svcdiscover

type Config struct {
	Label   string      `json:"label" yaml:"label"`
	Type    string      `json:"type" yaml:"type"`
	Enabled bool        `json:"enabled" yaml:"enabled"`
	Plugin  any         `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Nacos   NacosConfig `json:"nacos" yaml:"nacos"`
}

func NewConfig() Config {
	return Config{
		Label:   "",
		Type:    "service_discover",
		Enabled: false,
		Plugin:  nil,
		Nacos:   NewNacosConfig(),
	}
}
