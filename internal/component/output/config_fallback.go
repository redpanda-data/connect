package output

// TryConfig contains configuration fields for the Try output type.
type TryConfig []Config

// NewTryConfig creates a new BrokerConfig with default values.
func NewTryConfig() TryConfig {
	return TryConfig{}
}
