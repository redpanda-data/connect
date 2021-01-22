package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrometheusNoPushGateway(t *testing.T) {
	config := NewConfig()

	p, err := NewPrometheus(config)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.Nil(t, p.(*Prometheus).pusher)
}

func TestPrometheusWithPushGateway(t *testing.T) {
	pusherChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		pusherChan <- struct{}{}
	}))
	defer server.Close()

	config := NewConfig()
	config.Prometheus.PushURL = server.URL

	p, err := NewPrometheus(config)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, p.(*Prometheus).pusher)

	go func() {
		err = p.Close()
		assert.NoError(t, err)
	}()

	// Wait for message for the PushGateway after close
	select {
	case <-pusherChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "PushGateway did not receive expected messages")
	}
}

func TestPrometheusWithPushGatewayAndPushInterval(t *testing.T) {
	pusherChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		pusherChan <- struct{}{}
	}))
	defer server.Close()

	pushInterval := 1 * time.Millisecond
	config := NewConfig()
	config.Prometheus.PushURL = server.URL
	config.Prometheus.PushInterval = pushInterval.String()

	p, err := NewPrometheus(config)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, p.(*Prometheus).pusher)

	// Wait for first message for the PushGateway
	select {
	case <-pusherChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "PushGateway did not receive expected messages")
	}

	go func() {
		err = p.Close()
		assert.NoError(t, err)
	}()

	// Wait for another message for the PushGateway (might not be the one sent on close)
	select {
	case <-pusherChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "PushGateway did not receive expected messages after close")
	}
}
