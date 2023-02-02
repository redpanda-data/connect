package svcdiscover

type NacosConfig struct {
	ServerAddr  string `json:"server_addr" yaml:"server_addr"`
	ServerPort  int    `json:"server_port" yaml:"server_port"`
	Namespace   string `json:"namespace" yaml:"namespace"`
	ServiceName string `json:"service_name" yaml:"service_name"`
	RegistryIp  string `json:"registry_ip" yaml:"registry_ip"`
}

func NewNacosConfig() NacosConfig {
	return NacosConfig{
		ServerAddr:  "",
		ServerPort:  8848,
		Namespace:   "public",
		ServiceName: "benthos",
		RegistryIp:  "",
	}
}
