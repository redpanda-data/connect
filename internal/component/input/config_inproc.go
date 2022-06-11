package input

// InprocConfig is a configuration type for the inproc input.
type InprocConfig string

// NewInprocConfig creates a new inproc input config.
func NewInprocConfig() InprocConfig {
	return InprocConfig("")
}
