package input

type BeanstalkdConfig struct {
	Address string `json:"tcp_address" yaml:"tcp_address"`
}

func NewBeanstalkdConfig() BeanstalkdConfig {
	return BeanstalkdConfig{
		Address: "",
	}
}
