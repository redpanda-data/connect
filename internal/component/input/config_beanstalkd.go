package input

// BeanstalkdConfig contains configuration fields for the Beanstalkd input type.
type BeanstalkdConfig struct {
	Address string `json:"tcp_address" yaml:"tcp_address"`
}

// NewBeanstalkdConfig creates a new BeanstalkdConfig with default values.
func NewBeanstalkdConfig() BeanstalkdConfig {
	return BeanstalkdConfig{
		Address: "",
	}
}
