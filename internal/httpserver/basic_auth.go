package httpserver

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
	scryptN      = 32768
	scryptR      = 8
	scryptP      = 1
	scryptKeyLen = 32
)

// BasicAuthConfig contains struct based fields for basic authentication.
type BasicAuthConfig struct {
	Enabled      bool   `json:"enabled" yaml:"enabled"`
	Username     string `json:"username" yaml:"username"`
	PasswordHash string `json:"password_hash" yaml:"password_hash"`
	Realm        string `json:"realm" yaml:"realm"`
	Algorithm    string `json:"algorithm" yaml:"algorithm"`
	Salt         string `json:"salt" yaml:"salt"`
}

// NewBasicAuthConfig returns a BasicAuthConfig with default values.
func NewBasicAuthConfig() BasicAuthConfig {
	return BasicAuthConfig{
		Enabled:      false,
		Username:     "",
		PasswordHash: "",
		Realm:        "restricted",
		Algorithm:    "sha256",
		Salt:         "",
	}
}

// Validate confirms that the BasicAuth is properly configured.
func (b BasicAuthConfig) Validate() error {
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
func (b BasicAuthConfig) WrapHandler(next http.HandlerFunc) http.HandlerFunc {
	if !b.Enabled {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok {
			user = ""
			pass = ""
		}

		if ok, err := b.matches(user, pass); !ok || err != nil {
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm=%q, charset="UTF-8"`, b.Realm))
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// BasicAuthFieldSpec returns the spec for an HTTP BasicAuth component.
func BasicAuthFieldSpec() docs.FieldSpec {
	return docs.FieldObject("basic_auth", "Allows you to enforce and customise basic authentication for requests to the HTTP server.").WithChildren(
		docs.FieldBool("enabled", "Enable basic authentication").HasDefault(false),
		docs.FieldString("realm", "Custom realm name").HasDefault("restricted"),
		docs.FieldString("username", "Username required to authenticate.").HasDefault(""),
		docs.FieldString("password_hash", "Hashed password required to authenticate. (base64 encoded)").HasDefault(""),
		docs.FieldString("algorithm", "Encryption algorithm used to generate `password_hash`.", "md5", "sha256", "bcrypt", "scrypt").HasDefault("sha256"),
		docs.FieldString("salt", "Salt for scrypt algorithm. (base64 encoded)").HasDefault(""),
	).Advanced()
}

func (b BasicAuthConfig) matches(user, pass string) (bool, error) {
	expectedPassHash, err := base64.StdEncoding.DecodeString(b.PasswordHash)
	if err != nil {
		return false, err
	}

	userMatch := (subtle.ConstantTimeCompare([]byte(user), []byte(b.Username)) == 1)
	passMatch := b.compareHashAndPassword(expectedPassHash, []byte(pass))

	return (userMatch && passMatch), nil
}

func (b BasicAuthConfig) compareHashAndPassword(hashedPassword, password []byte) bool {
	switch b.Algorithm {
	case "md5":
		v := md5.Sum(password)
		return (subtle.ConstantTimeCompare(hashedPassword, v[:]) == 1)
	case "sha256":
		v := sha256.Sum256(password)
		return (subtle.ConstantTimeCompare(hashedPassword, v[:]) == 1)
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

		v, err := scrypt.Key(password, salt, scryptN, scryptR, scryptP, scryptKeyLen)
		if err != nil {
			return false
		}
		return (subtle.ConstantTimeCompare(hashedPassword, v) == 1)
	default:
		return false
	}
}
