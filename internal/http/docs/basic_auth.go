package docs

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// BasicAuth contains the configuration fields for the HTTP BasicAuth
type BasicAuth struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// NewBasicAuth returns a BasicAuth with default values
func NewBasicAuth() BasicAuth {
	return BasicAuth{
		Username: "",
		Password: "",
	}
}

// WrapHandler wraps the provided HTTP handler with middleware that enforces
// BasicAuth if it's enabled.
func (b BasicAuth) WrapHandler(next http.HandlerFunc) http.HandlerFunc {
	if !b.enabled() {
		return next
	}

	return func(next http.HandlerFunc, auth BasicAuth) http.HandlerFunc {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !(ok && auth.matches(user, pass)) {
				w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}(next, b)
}

// BasicAuthFieldSpec returns the spec for an HTTP BasicAuth component
func BasicAuthFieldSpec() docs.FieldSpec {
	return docs.FieldObject("basic_auth", "Require HTTP Basic Auth for the HTTP server.").WithChildren(
		docs.FieldString("username", "Basic Auth Username").HasDefault(""),
		docs.FieldString("password", "Basic Auth Password").HasDefault(""),
	).Advanced()
}

func (b BasicAuth) enabled() bool {
	return b.Username != "" && b.Password != ""
}

func (b BasicAuth) matches(user, pass string) bool {
	userHash := sha256.Sum256([]byte(user))
	passHash := sha256.Sum256([]byte(pass))
	expectedUserHash := sha256.Sum256([]byte(b.Username))
	expectedPassHash := sha256.Sum256([]byte(b.Password))

	userMatch := (subtle.ConstantTimeCompare(userHash[:], expectedUserHash[:]) == 1)
	passMatch := (subtle.ConstantTimeCompare(passHash[:], expectedPassHash[:]) == 1)

	return userMatch && passMatch
}
