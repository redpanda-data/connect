package output

// BeanstalkdConfig contains configuration fields for the Beanstalkd output type.
type BeanstalkdConfig struct {
	Address     string `json:"tcp_address" yaml:"tcp_address"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewBeanstalkdConfig creates a new BeanstalkdConfig with default values.
func NewBeanstalkdConfig() BeanstalkdConfig {
	return BeanstalkdConfig{
		Address:     "",
		MaxInFlight: 64,
	}
}
