package docs

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/scrypt"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

const (
	SCRYPT_N      = 32768
	SCRYPT_r      = 8
	SCRYPT_p      = 1
	SCRYPT_KeyLen = 32
)

// BasicAuth contains the configuration fields for the HTTP BasicAuth
type BasicAuth struct {
	Enabled      bool   `json:"enabled" yaml:"enabled"`
	Username     string `json:"username" yaml:"username"`
	PasswordHash string `json:"password_hash" yaml:"password_hash"`
	Realm        string `json:"realm" yaml:"realm"`
	Algorithm    string `json:"algorithm" yaml:"algorithm"`
	Salt         string `json:"salt" yaml:"salt"`
}

// NewBasicAuth returns a BasicAuth with default values
func NewBasicAuth() BasicAuth {
	return BasicAuth{
		Enabled:      false,
		Username:     "",
		PasswordHash: "",
		Realm:        "restricted",
		Algorithm:    "sha256",
		Salt:         "",
	}
}

// Validate confirms that the BasicAuth is properly configured
func (b BasicAuth) Validate() error {
	if !b.Enabled {
		return nil
	}

	if b.Username == "" || b.PasswordHash == "" {
		return errors.New("both username and password_hash are required")
	}

	if !(b.Algorithm == "md5" || b.Algorithm == "sha256" || b.Algorithm == "bcrypt" || b.Algorithm == "scrypt") {
		return errors.New("algorithm should be one of md5, sha256, bcrypt, or scrypt")
	}

	if b.Algorithm == "scrypt" && b.Salt == "" {
		return errors.New("salt is required for scrypt")
	}

	if b.Algorithm == "scrypt" {
		if _, err := base64.StdEncoding.DecodeString(b.Salt); err != nil {
			return fmt.Errorf("invalid salt : %w", err)
		}
	}

	return nil
}

// WrapHandler wraps the provided HTTP handler with middleware that enforces
// BasicAuth if it's enabled.
func (b BasicAuth) WrapHandler(next http.HandlerFunc) http.HandlerFunc {
	if !b.Enabled {
		return next
	}

	return func(next http.HandlerFunc, auth BasicAuth) http.HandlerFunc {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok {
				user = ""
				pass = ""
			}

			if ok, err := auth.matches(user, pass); !ok || err != nil {
				if err != nil {
					http.Error(w, "Internal Server Error", http.StatusInternalServerError)
					return
				}

				w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm="%s", charset="UTF-8"`, b.Realm))
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
		docs.FieldBool("enabled", "Enable Basic Auth").HasDefault(false),
		docs.FieldString("realm", "Custom realm name").HasDefault("restricted"),
		docs.FieldString("username", "Basic Auth Username").HasDefault(""),
		docs.FieldString("password_hash", "Basic Auth Password Hash").HasDefault(""),
		docs.FieldString("algorithm", "Which hasing algorithm was used to hash the password").HasDefault("sha256"),
		docs.FieldString("salt", "Salt for scrypt hasing algorithm").HasDefault(""),
	).Advanced()
}

func (b BasicAuth) matches(user, pass string) (bool, error) {
	expectedPassHash, err := base64.StdEncoding.DecodeString(b.PasswordHash)
	if err != nil {
		return false, err
	}

	userMatch := (subtle.ConstantTimeCompare([]byte(user), []byte(b.Username)) == 1)
	passMatch := b.compareHashAndPassword(expectedPassHash, []byte(pass))

	return (userMatch && passMatch), nil
}

func (b BasicAuth) compareHashAndPassword(hashedPassword, password []byte) bool {
	switch b.Algorithm {
	case "md5":
		v := md5.Sum(password)
		return (subtle.ConstantTimeCompare(hashedPassword[:], v[:]) == 1)
	case "sha256":
		v := sha256.Sum256(password)
		return (subtle.ConstantTimeCompare(hashedPassword[:], v[:]) == 1)
	case "bcrypt":
		if err := bcrypt.CompareHashAndPassword(hashedPassword, password); err != nil {
			return false
		}
		return true
	case "scrypt":
		salt, err := base64.StdEncoding.DecodeString(b.Salt)
		if err != nil {
			return false
		}

		v, err := scrypt.Key(password, salt, SCRYPT_N, SCRYPT_r, SCRYPT_p, SCRYPT_KeyLen)
		if err != nil {
			return false
		}
		return (subtle.ConstantTimeCompare(hashedPassword[:], v[:]) == 1)
	default:
		return false
	}
}
