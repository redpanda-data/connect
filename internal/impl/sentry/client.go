package sentry

import "github.com/getsentry/sentry-go"

type clientOptionsFunc func(opts *sentry.ClientOptions) *sentry.ClientOptions

func withTransport(t sentry.Transport) clientOptionsFunc {
	return func(opts *sentry.ClientOptions) *sentry.ClientOptions {
		opts.Transport = t

		return opts
	}
}
