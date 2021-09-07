package auth

import "github.com/nats-io/nats.go"

// GetOptions initialises NATS functional options for the auth fields
func GetOptions(auth Config) []nats.Option {
	var opts []nats.Option
	if auth.NKeyFile != "" {
		if opt, err := nats.NkeyOptionFromSeed(auth.NKeyFile); err != nil {
			opts = append(opts, func(*nats.Options) error { return err })
		} else {
			opts = append(opts, opt)
		}
	}

	if auth.UserCredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(auth.UserCredentialsFile))
	}
	return opts
}
