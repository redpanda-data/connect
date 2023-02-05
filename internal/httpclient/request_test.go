package httpclient

import (
	"net/http"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/golang-jwt/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleMessageHeaders(t *testing.T) {
	oldConf := NewOldConfig()
	oldConf.Headers["Content-Type"] = "foo"
	oldConf.Metadata.IncludePrefixes = []string{"more_"}

	reqCreator, err := RequestCreatorFromOldConfig(oldConf, mock.NewManager())
	require.NoError(t, err)

	part := message.NewPart([]byte("hello world"))
	part.MetaSetMut("more_bar", "barvalue")
	part.MetaSetMut("ignore_baz", "bazvalue")

	b := message.Batch{part}

	req, err := reqCreator.Create(b)
	require.NoError(t, err)

	assert.Equal(t, []string{"foo"}, req.Header.Values("Content-Type"))
	assert.Equal(t, []string{"barvalue"}, req.Header.Values("more_bar"))
	assert.Equal(t, []string(nil), req.Header.Values("ignore_baz"))
}

type jwtDynamicClaimsTester struct {
	JWTConfig
	resolvedClaims map[string]any
}

func (t *jwtDynamicClaimsTester) SignWithClaims(_ ifs.FS, _ *http.Request, claims jwt.MapClaims) error {
	for k, v := range claims {
		t.resolvedClaims[k] = v
	}
	return nil
}

func TestDynamicJWTClaims(t *testing.T) {
	mockJWTConf := jwtDynamicClaimsTester{
		JWTConfig:      NewJWTConfig(),
		resolvedClaims: map[string]any{},
	}

	oldConf := NewOldConfig()
	oldConf.AuthConfig.JWT = mockJWTConf.JWTConfig
	oldConf.AuthConfig.JWT.Enabled = true
	oldConf.AuthConfig.JWT.Claims["static_string"] = "abc"
	oldConf.AuthConfig.JWT.Claims["static_number"] = 123
	oldConf.AuthConfig.JWT.Claims["dynamic_string"] = "test-${! meta(\"foo\")}"
	oldConf.AuthConfig.JWT.Claims["dynamic_number"] = "${! meta(\"foo\")}"

	reqCreator, err := RequestCreatorFromOldConfig(oldConf, mock.NewManager())
	require.NoError(t, err)
	reqCreator.jwtSigner = &mockJWTConf

	part := message.NewPart([]byte("hello world"))
	part.MetaSetMut("foo", 456)

	b := message.Batch{part}

	_, err = reqCreator.Create(b)
	require.NoError(t, err)

	// Validate that the claims are interpolated as expected.
	assert.Equal(t, len(mockJWTConf.resolvedClaims), len(oldConf.JWT.Claims))
	assert.Equal(t, mockJWTConf.resolvedClaims, map[string]any{
		"static_string":  "abc",
		"static_number":  123,
		"dynamic_string": "test-456",
		"dynamic_number": 456.0, // should interpolate to a float64
	})
}
