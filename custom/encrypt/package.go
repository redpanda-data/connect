package bloblang

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func EncryptGCM(plainText string, password string) (string, error) {
	key, _ := hex.DecodeString(password)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, aesgcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	authTag := make([]byte, 16)
	if _, err = io.ReadFull(rand.Reader, authTag); err != nil {
		panic(err.Error())
	}

	// Add nonce as the prefix, so we don't have to save the nonce separately
	// cipherText := aesgcm.Seal(nonce, nonce, []byte(plainText), nil)
	cipherText := aesgcm.Seal(nonce, nonce, []byte(plainText), authTag)
	return fmt.Sprintf("%x", cipherText), nil
}

func init() {
	crazyObjectSpec := bloblang.NewPluginSpec().
		Param(bloblang.NewStringParam("plaintext"))

	err := bloblang.RegisterFunctionV2("aes_gcm_encrypt", crazyObjectSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		plaintext, err := args.GetString("plaintext")
		if err != nil {
			return nil, err
		}

		return func() (interface{}, error) {
			encrypted, err := EncryptGCM(plaintext, os.Getenv("API_PROVIDER_ENCRYPTION_KEY"))

			if err != nil {
				return nil, err
			}

			return encrypted, nil
		}, nil
	})
	if err != nil {
		panic(err)
	}

	// intoObjectSpec := bloblang.NewPluginSpec().
	// 	Param(bloblang.NewStringParam("key"))

	// err = bloblang.RegisterMethodV2("into_object", intoObjectSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
	// 	key, err := args.GetString("key")
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	return func(v interface{}) (interface{}, error) {
	// 		return map[string]interface{}{key: v}, nil
	// 	}, nil
	// })
	// if err != nil {
	// 	panic(err)
	// }
}
