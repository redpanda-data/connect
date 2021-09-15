package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIEnableCORS(t *testing.T) {
	conf := NewConfig()
	conf.EnableCORS = true

	s, err := New("", "", conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	handler := s.server.Handler

	request, _ := http.NewRequest("OPTIONS", "/version", nil)
	request.Header.Add("Origin", "meow")
	request.Header.Add("Access-Control-Request-Method", "POST")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
}
