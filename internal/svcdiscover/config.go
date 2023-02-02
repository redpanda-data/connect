package svcdiscover

type Config struct {
	Label   string      `json:"label" yaml:"label"`
	Type    string      `json:"type" yaml:"type"`
	Enabled bool        `json:"enabled" yaml:"enabled"`
	Nacos   NacosConfig `json:"nacos" yaml:"nacos"`
}

func NewConfig() Config {
	return Config{
		Label:   "",
		Type:    "service_discover",
		Enabled: false,
		Nacos:   NewNacosConfig(),
	}
}
