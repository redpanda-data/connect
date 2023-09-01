package sentry

import (
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/mock"
)

var (
	argEvent = mock.AnythingOfType("*sentry.Event")
)

type mockTransport struct {
	mock.Mock
}

func (t *mockTransport) Flush(timeout time.Duration) bool {
	args := t.Called(timeout)

	return args.Bool(0)
}
func (t *mockTransport) Configure(options sentry.ClientOptions) {
	t.Called(options)
}
func (t *mockTransport) SendEvent(event *sentry.Event) {
	t.Called(event)
}
