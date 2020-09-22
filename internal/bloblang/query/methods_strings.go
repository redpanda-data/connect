package query

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/ascii85"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io/ioutil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/microcosm-cc/bluemonday"
	"github.com/tilinna/z85"
)

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"capitalize", "",
	).InCategory(
		MethodCategoryStrings,
		"Takes a string value and returns a copy with all Unicode letters that begin words mapped to their Unicode title case.",
		NewExampleSpec("",
			`root.title = this.title.capitalize()`,
			`{"title":"the foo bar"}`,
			`{"title":"The Foo Bar"}`,
		),
	),
	false, capitalizeMethod,
	ExpectNArgs(0),
)

func capitalizeMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strings.Title(t), nil
		case []byte:
			return bytes.Title(t), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"encode", "",
	).InCategory(
		MethodCategoryEncoding,
		"Encodes a string or byte array target according to a chosen scheme and returns a string result. Available schemes are: `base64`, `base64url`, `hex`, `ascii85`, `z85`.",
		NewExampleSpec("",
			`root.encoded = this.value.encode("hex")`,
			`{"value":"hello world"}`,
			`{"encoded":"68656c6c6f20776f726c64"}`,
		),
	),
	true, encodeMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func encodeMethod(target Function, args ...interface{}) (Function, error) {
	var schemeFn func([]byte) (string, error)
	switch args[0].(string) {
	case "base64":
		schemeFn = func(b []byte) (string, error) {
			var buf bytes.Buffer
			e := base64.NewEncoder(base64.StdEncoding, &buf)
			e.Write(b)
			e.Close()
			return buf.String(), nil
		}
	case "base64url":
		schemeFn = func(b []byte) (string, error) {
			var buf bytes.Buffer
			e := base64.NewEncoder(base64.URLEncoding, &buf)
			e.Write(b)
			e.Close()
			return buf.String(), nil
		}
	case "hex":
		schemeFn = func(b []byte) (string, error) {
			var buf bytes.Buffer
			e := hex.NewEncoder(&buf)
			if _, err := e.Write(b); err != nil {
				return "", err
			}
			return buf.String(), nil
		}
	case "ascii85":
		schemeFn = func(b []byte) (string, error) {
			if len(b)%4 != 0 {
				return "", z85.ErrLength
			}
			var buf bytes.Buffer
			e := ascii85.NewEncoder(&buf)
			if _, err := e.Write(b); err != nil {
				return "", err
			}
			return buf.String(), nil
		}
	case "z85":
		schemeFn = func(b []byte) (string, error) {
			enc := make([]byte, z85.EncodedLen(len(b)))
			if _, err := z85.Encode(enc, b); err != nil {
				return "", err
			}
			return string(enc), nil
		}
	default:
		return nil, fmt.Errorf("unrecognized encoding type: %v", args[0])
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res string
		var err error
		switch t := v.(type) {
		case string:
			res, err = schemeFn([]byte(t))
		case []byte:
			res, err = schemeFn(t)
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"decode", "",
	).InCategory(
		MethodCategoryEncoding,
		"Decodes an encoded string target according to a chosen scheme and returns the result as a byte array. When mapping the result to a JSON field the value should be cast to a string using the method [`string`][methods.string], or encoded using the method [`encode`][methods.encode], otherwise it will be base64 encoded by default.\n\nAvailable schemes are: `base64`, `base64url`, `hex`, `ascii85`, `z85`.",
		NewExampleSpec("",
			`root.decoded = this.value.decode("hex").string()`,
			`{"value":"68656c6c6f20776f726c64"}`,
			`{"decoded":"hello world"}`,
		),
	),
	true, decodeMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func decodeMethod(target Function, args ...interface{}) (Function, error) {
	var schemeFn func([]byte) ([]byte, error)
	switch args[0].(string) {
	case "base64":
		schemeFn = func(b []byte) ([]byte, error) {
			e := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(b))
			return ioutil.ReadAll(e)
		}
	case "base64url":
		schemeFn = func(b []byte) ([]byte, error) {
			e := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(b))
			return ioutil.ReadAll(e)
		}
	case "hex":
		schemeFn = func(b []byte) ([]byte, error) {
			e := hex.NewDecoder(bytes.NewReader(b))
			return ioutil.ReadAll(e)
		}
	case "ascii85":
		schemeFn = func(b []byte) ([]byte, error) {
			e := ascii85.NewDecoder(bytes.NewReader(b))
			return ioutil.ReadAll(e)
		}
	case "z85":
		schemeFn = func(b []byte) ([]byte, error) {
			dec := make([]byte, z85.DecodedLen(len(b)))
			if _, err := z85.Decode(dec, b); err != nil {
				return nil, err
			}
			return dec, nil
		}
	default:
		return nil, fmt.Errorf("unrecognized encoding type: %v", args[0])
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res []byte
		var err error
		switch t := v.(type) {
		case string:
			res, err = schemeFn([]byte(t))
		case []byte:
			res, err = schemeFn(t)
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"encrypt_aes", "",
	).InCategory(
		MethodCategoryEncoding,
		"Encrypts a string or byte array target according to a chosen AES encryption method and returns a string result. The algorithms require a key and an initialization vector / nonce. Available schemes are: `ctr`, `ofb`, `cbc`.",
		NewExampleSpec("",
			`let key = "2b7e151628aed2a6abf7158809cf4f3c".decode("hex")
let vector = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff".decode("hex")
root.encrypted = this.value.encrypt_aes("ctr", $key, $vector).encode("hex")`,
			`{"value":"hello world!"}`,
			`{"encrypted":"84e9b31ff7400bdf80be7254"}`,
		),
	),
	true, encryptAESMethod,
	ExpectNArgs(3),
	ExpectAllStringArgs(),
)

func encryptAESMethod(target Function, args ...interface{}) (Function, error) {
	key := []byte(args[1].(string))
	iv := []byte(args[2].(string))

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	var schemeFn func([]byte) (string, error)
	switch args[0].(string) {
	case "ctr":
		schemeFn = func(b []byte) (string, error) {
			ciphertext := make([]byte, len(b))
			stream := cipher.NewCTR(block, iv)
			stream.XORKeyStream(ciphertext, b)
			return string(ciphertext), nil
		}
	case "ofb":
		schemeFn = func(b []byte) (string, error) {
			ciphertext := make([]byte, len(b))
			stream := cipher.NewOFB(block, iv)
			stream.XORKeyStream(ciphertext, b)
			return string(ciphertext), nil
		}
	case "cbc":
		schemeFn = func(b []byte) (string, error) {
			if len(b)%aes.BlockSize != 0 {
				return "", fmt.Errorf("plaintext is not a multiple of the block size")
			}

			ciphertext := make([]byte, len(b))
			stream := cipher.NewCBCEncrypter(block, iv)
			stream.CryptBlocks(ciphertext, b)
			return string(ciphertext), nil
		}
	default:
		return nil, fmt.Errorf("unrecognized encryption type: %v", args[0])
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res string
		var err error
		switch t := v.(type) {
		case string:
			res, err = schemeFn([]byte(t))
		case []byte:
			res, err = schemeFn(t)
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"decrypt_aes", "",
	).InCategory(
		MethodCategoryEncoding,
		"Decrypts an encrypted string or byte array target according to a chosen AES encryption method and returns the result as a byte array. The algorithms require a key and an initialization vector / nonce. Available schemes are: `ctr`, `ofb`, `cbc`.",
		NewExampleSpec("",
			`let key = "2b7e151628aed2a6abf7158809cf4f3c".decode("hex")
let vector = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff".decode("hex")
root.decrypted = this.value.decode("hex").decrypt_aes("ctr", $key, $vector).string()`,
			`{"value":"84e9b31ff7400bdf80be7254"}`,
			`{"decrypted":"hello world!"}`,
		),
	),
	true, decryptAESMethod,
	ExpectNArgs(3),
	ExpectAllStringArgs(),
)

func decryptAESMethod(target Function, args ...interface{}) (Function, error) {
	key := []byte(args[1].(string))
	iv := []byte(args[2].(string))

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	var schemeFn func([]byte) ([]byte, error)
	switch args[0].(string) {
	case "ctr":
		schemeFn = func(b []byte) ([]byte, error) {
			plaintext := make([]byte, len(b))
			stream := cipher.NewCTR(block, iv)
			stream.XORKeyStream(plaintext, b)
			return plaintext, nil
		}
	case "ofb":
		schemeFn = func(b []byte) ([]byte, error) {
			plaintext := make([]byte, len(b))
			stream := cipher.NewOFB(block, iv)
			stream.XORKeyStream(plaintext, b)
			return plaintext, nil
		}
	case "cbc":
		schemeFn = func(b []byte) ([]byte, error) {
			if len(b)%aes.BlockSize != 0 {
				return nil, fmt.Errorf("ciphertext is not a multiple of the block size")
			}
			stream := cipher.NewCBCDecrypter(block, iv)
			stream.CryptBlocks(b, b)
			return b, nil
		}
	default:
		return nil, fmt.Errorf("unrecognized decryption type: %v", args[0])
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res []byte
		var err error
		switch t := v.(type) {
		case string:
			res, err = schemeFn([]byte(t))
		case []byte:
			res, err = schemeFn(t)
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"escape_html", "",
	).InCategory(
		MethodCategoryStrings,
		"Escapes a string so that special characters like `<` to become `&lt;`. It escapes only five such characters: `<`, `>`, `&`, `'` and `\"` so that it can be safely placed within an HTML entity.",
		NewExampleSpec("",
			`root.escaped = this.value.escape_html()`,
			`{"value":"foo & bar"}`,
			`{"escaped":"foo &amp; bar"}`,
		),
	),
	false, escapeHTMLMethod,
	ExpectNArgs(0),
)

func escapeHTMLMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res string
		var err error
		switch t := v.(type) {
		case string:
			res = html.EscapeString(t)
		case []byte:
			res = html.EscapeString(string(t))
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"unescape_html", "",
	).InCategory(
		MethodCategoryStrings,
		"Unescapes a string so that entities like `&lt;` become `<`. It unescapes a larger range of entities than `escape_html` escapes. For example, `&aacute;` unescapes to `á`, as does `&#225;` and `&xE1;`.",
		NewExampleSpec("",
			`root.unescaped = this.value.unescape_html()`,
			`{"value":"foo &amp; bar"}`,
			`{"unescaped":"foo & bar"}`,
		),
	),
	false, unescapeHTMLMethod,
	ExpectNArgs(0),
)

func unescapeHTMLMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res string
		var err error
		switch t := v.(type) {
		case string:
			res = html.UnescapeString(t)
		case []byte:
			res = html.UnescapeString(string(t))
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"escape_url_query", "",
	).InCategory(
		MethodCategoryStrings,
		"Escapes a string so that it can be safely placed within a URL query.",
		NewExampleSpec("",
			`root.escaped = this.value.escape_url_query()`,
			`{"value":"foo & bar"}`,
			`{"escaped":"foo+%26+bar"}`,
		),
	),
	false, escapeURLQueryMethod,
	ExpectNArgs(0),
)

func escapeURLQueryMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res string
		var err error
		switch t := v.(type) {
		case string:
			res = url.QueryEscape(t)
		case []byte:
			res = url.QueryEscape(string(t))
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"unescape_url_query", "",
	).InCategory(
		MethodCategoryStrings,
		"Expands escape sequences from a URL query string.",
		NewExampleSpec("",
			`root.unescaped = this.value.unescape_url_query()`,
			`{"value":"foo+%26+bar"}`,
			`{"unescaped":"foo & bar"}`,
		),
	),
	false, unescapeURLQueryMethod,
	ExpectNArgs(0),
)

func unescapeURLQueryMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res string
		var err error
		switch t := v.(type) {
		case string:
			res, err = url.QueryUnescape(t)
		case []byte:
			res, err = url.QueryUnescape(string(t))
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"format", "",
	).InCategory(
		MethodCategoryStrings,
		"Use a value string as a format specifier in order to produce a new string, using any number of provided arguments.",
		NewExampleSpec("",
			`root.foo = "%s(%v): %v".format(this.name, this.age, this.fingers)`,
			`{"name":"lance","age":37,"fingers":13}`,
			`{"foo":"lance(37): 13"}`,
		),
	),
	true, formatMethod,
)

func formatMethod(target Function, args ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return fmt.Sprintf(t, args...), nil
		case []byte:
			return fmt.Sprintf(string(t), args...), nil
		default:
			return nil, NewTypeError(v, ValueString)
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"has_prefix", "",
	).InCategory(
		MethodCategoryStrings,
		"Checks whether a string has a prefix argument and returns a bool.",
		NewExampleSpec("",
			`root.t1 = this.v1.has_prefix("foo")
root.t2 = this.v2.has_prefix("foo")`,
			`{"v1":"foobar","v2":"barfoo"}`,
			`{"t1":true,"t2":false}`,
		),
	),
	true, hasPrefixMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func hasPrefixMethod(target Function, args ...interface{}) (Function, error) {
	prefix := args[0].(string)
	prefixB := []byte(prefix)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strings.HasPrefix(t, prefix), nil
		case []byte:
			return bytes.HasPrefix(t, prefixB), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"has_suffix", "",
	).InCategory(
		MethodCategoryStrings,
		"Checks whether a string has a suffix argument and returns a bool.",
		NewExampleSpec("",
			`root.t1 = this.v1.has_suffix("foo")
root.t2 = this.v2.has_suffix("foo")`,
			`{"v1":"foobar","v2":"barfoo"}`,
			`{"t1":false,"t2":true}`,
		),
	),
	true, hasSuffixMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func hasSuffixMethod(target Function, args ...interface{}) (Function, error) {
	prefix := args[0].(string)
	prefixB := []byte(prefix)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strings.HasSuffix(t, prefix), nil
		case []byte:
			return bytes.HasSuffix(t, prefixB), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"hash", "",
	).InCategory(
		MethodCategoryEncoding,
		`
Hashes a string or byte array according to a chosen algorithm and returns the result as a byte array. When mapping the result to a JSON field the value should be cast to a string using the method `+"[`string`][methods.string], or encoded using the method [`encode`][methods.encode]"+`, otherwise it will be base64 encoded by default.

Available algorithms are: `+"`hmac_sha1`, `hmac_sha256`, `hmac_sha512`, `sha1`, `sha256`, `sha512`, `xxhash64`"+`.

The following algorithms require a key, which is specified as a second argument: `+"`hmac_sha1`, `hmac_sha256`, `hmac_sha512`"+`.`,
		NewExampleSpec("",
			`root.h1 = this.value.hash("sha1").encode("hex")
root.h2 = this.value.hash("hmac_sha1","static-key").encode("hex")`,
			`{"value":"hello world"}`,
			`{"h1":"2aae6c35c94fcfb415dbe95f408b9ce91ee846ed","h2":"d87e5f068fa08fe90bb95bc7c8344cb809179d76"}`,
		),
	),
	true, hashMethod,
	ExpectAtLeastOneArg(),
	ExpectStringArg(0),
	ExpectStringArg(1),
)

func hashMethod(target Function, args ...interface{}) (Function, error) {
	var key []byte
	if len(args) > 1 {
		key = []byte(args[1].(string))
	}
	var hashFn func([]byte) ([]byte, error)
	switch args[0].(string) {
	case "hmac_sha1", "hmac-sha1":
		if len(key) == 0 {
			return nil, fmt.Errorf("hash algorithm %v requires a key argument", args[0].(string))
		}
		hashFn = func(b []byte) ([]byte, error) {
			hasher := hmac.New(sha1.New, key)
			hasher.Write(b)
			return hasher.Sum(nil), nil
		}
	case "hmac_sha256", "hmac-sha256":
		if len(key) == 0 {
			return nil, fmt.Errorf("hash algorithm %v requires a key argument", args[0].(string))
		}
		hashFn = func(b []byte) ([]byte, error) {
			hasher := hmac.New(sha256.New, key)
			hasher.Write(b)
			return hasher.Sum(nil), nil
		}
	case "hmac_sha512", "hmac-sha512":
		if len(key) == 0 {
			return nil, fmt.Errorf("hash algorithm %v requires a key argument", args[0].(string))
		}
		hashFn = func(b []byte) ([]byte, error) {
			hasher := hmac.New(sha512.New, key)
			hasher.Write(b)
			return hasher.Sum(nil), nil
		}
	case "sha1":
		hashFn = func(b []byte) ([]byte, error) {
			hasher := sha1.New()
			hasher.Write(b)
			return hasher.Sum(nil), nil
		}
	case "sha256":
		hashFn = func(b []byte) ([]byte, error) {
			hasher := sha256.New()
			hasher.Write(b)
			return hasher.Sum(nil), nil
		}
	case "sha512":
		hashFn = func(b []byte) ([]byte, error) {
			hasher := sha512.New()
			hasher.Write(b)
			return hasher.Sum(nil), nil
		}
	case "xxhash64":
		hashFn = func(b []byte) ([]byte, error) {
			h := xxhash.New64()
			h.Write(b)
			return []byte(strconv.FormatUint(h.Sum64(), 10)), nil
		}
	default:
		return nil, fmt.Errorf("unrecognized hash type: %v", args[0])
	}

	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var res []byte
		var err error
		switch t := v.(type) {
		case string:
			res, err = hashFn([]byte(t))
		case []byte:
			res, err = hashFn(t)
		default:
			err = NewTypeError(v, ValueString)
		}
		return res, err
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"join", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Join an array of strings with an optional delimiter into a single string.",
		NewExampleSpec("",
			`root.joined_words = this.words.join()
root.joined_numbers = this.numbers.map_each(this.string()).join(",")`,
			`{"words":["hello","world"],"numbers":[3,8,11]}`,
			`{"joined_numbers":"3,8,11","joined_words":"helloworld"}`,
		),
	),
	true, joinMethod,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func joinMethod(target Function, args ...interface{}) (Function, error) {
	var delim string
	if len(args) > 0 {
		delim = args[0].(string)
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		slice, ok := v.([]interface{})
		if !ok {
			return nil, NewTypeError(v, ValueArray)
		}

		var buf bytes.Buffer
		for i, sv := range slice {
			if i > 0 {
				buf.WriteString(delim)
			}
			switch t := sv.(type) {
			case string:
				buf.WriteString(t)
			case []byte:
				buf.Write(t)
			default:
				return nil, fmt.Errorf("failed to join element %v: %w", i, NewTypeError(sv, ValueString))
			}
		}

		return buf.String(), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"uppercase", "",
	).InCategory(
		MethodCategoryStrings,
		"Convert a string value into uppercase.",
		NewExampleSpec("",
			`root.foo = this.foo.uppercase()`,
			`{"foo":"hello world"}`,
			`{"foo":"HELLO WORLD"}`,
		),
	),
	false, uppercaseMethod,
	ExpectNArgs(0),
)

func uppercaseMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strings.ToUpper(t), nil
		case []byte:
			return bytes.ToUpper(t), nil
		default:
			return nil, &ErrRecoverable{
				Recovered: strings.ToUpper(IToString(v)),
				Err:       NewTypeError(v, ValueString),
			}
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"lowercase", "",
	).InCategory(
		MethodCategoryStrings,
		"Convert a string value into lowercase.",
		NewExampleSpec("",
			`root.foo = this.foo.lowercase()`,
			`{"foo":"HELLO WORLD"}`,
			`{"foo":"hello world"}`,
		),
	),
	false, lowercaseMethod,
	ExpectNArgs(0),
)

func lowercaseMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strings.ToLower(t), nil
		case []byte:
			return bytes.ToLower(t), nil
		default:
			return nil, &ErrRecoverable{
				Recovered: strings.ToLower(IToString(v)),
				Err:       NewTypeError(v, ValueString),
			}
		}
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"parse_csv", "",
	).InCategory(
		MethodCategoryParsing,
		"Attempts to parse a string into an array of objects by following the CSV format described in RFC 4180. The first line is assumed to be a header row, which determines the keys of values in each object.",
		NewExampleSpec("",
			`root.orders = this.orders.parse_csv()`,
			`{"orders":"foo,bar\nfoo 1,bar 1\nfoo 2,bar 2"}`,
			`{"orders":[{"bar":"bar 1","foo":"foo 1"},{"bar":"bar 2","foo":"foo 2"}]}`,
		),
	),
	false, parseCSVMethod,
	ExpectNArgs(0),
)

func parseCSVMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var csvBytes []byte
		switch t := v.(type) {
		case string:
			csvBytes = []byte(t)
		case []byte:
			csvBytes = t
		default:
			return nil, NewTypeError(v, ValueString)
		}

		r := csv.NewReader(bytes.NewReader(csvBytes))
		strRecords, err := r.ReadAll()
		if err != nil {
			return nil, err
		}
		if len(strRecords) == 0 {
			return nil, errors.New("zero records were parsed")
		}

		records := make([]interface{}, 0, len(strRecords)-1)
		headers := strRecords[0]
		if len(headers) == 0 {
			return nil, fmt.Errorf("no headers found on first row")
		}
		for j, strRecord := range strRecords[1:] {
			if len(headers) != len(strRecord) {
				return nil, fmt.Errorf("record on line %v: record mismatch with headers", j)
			}
			obj := make(map[string]interface{}, len(strRecord))
			for i, r := range strRecord {
				obj[headers[i]] = r
			}
			records = append(records, obj)
		}

		return records, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"parse_json", "",
	).InCategory(
		MethodCategoryParsing,
		"Attempts to parse a string as a JSON document and returns the result.",
		NewExampleSpec("",
			`root.doc = this.doc.parse_json()`,
			`{"doc":"{\"foo\":\"bar\"}"}`,
			`{"doc":{"foo":"bar"}}`,
		),
	),
	false, parseJSONMethod,
	ExpectNArgs(0),
)

func parseJSONMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var jsonBytes []byte
		switch t := v.(type) {
		case string:
			jsonBytes = []byte(t)
		case []byte:
			jsonBytes = t
		default:
			return nil, NewTypeError(v, ValueString)
		}
		var jObj interface{}
		if err := json.Unmarshal(jsonBytes, &jObj); err != nil {
			return nil, fmt.Errorf("failed to parse value as JSON: %w", err)
		}
		return jObj, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"parse_timestamp_unix", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to parse a string as a timestamp, following ISO 8601 format by default, and returns the unix epoch.",
		NewExampleSpec("",
			`root.doc.timestamp = this.doc.timestamp.parse_timestamp_unix()`,
			`{"doc":{"timestamp":"2020-08-14T11:45:26.371Z"}}`,
			`{"doc":{"timestamp":1597405526}}`,
		),
		NewExampleSpec(
			"An optional string argument can be used in order to specify the expected format of the timestamp. The format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value.",
			`root.doc.timestamp = this.doc.timestamp.parse_timestamp_unix("2006-Jan-02")`,
			`{"doc":{"timestamp":"2020-Aug-14"}}`,
			`{"doc":{"timestamp":1597363200}}`,
		),
	),
	true, parseTimestampMethod,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func parseTimestampMethod(target Function, args ...interface{}) (Function, error) {
	layout := time.RFC3339
	if len(args) > 0 {
		layout = args[0].(string)
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var str string
		switch t := v.(type) {
		case []byte:
			str = string(t)
		case string:
			str = t
		default:
			return nil, NewTypeError(v, ValueString)
		}
		ut, err := time.Parse(layout, str)
		if err != nil {
			return nil, err
		}
		return ut.Unix(), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"format_timestamp", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to format a unix timestamp as a string, following ISO 8601 format by default.",
		NewExampleSpec("",
			`root.something_at = (this.created_at + 300).format_timestamp()`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-08-14T11:50:26.371Z"}`,
		),
		NewExampleSpec(
			"An optional string argument can be used in order to specify the expected format of the timestamp. The format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value.",
			`root.something_at = (this.created_at + 300).format_timestamp("2006-Jan-02 15:04:05")`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-Aug-14 11:50:26"}`,
		),
		NewExampleSpec(
			"A second optional string argument can also be used in order to specify a timezone, otherwise the local timezone is used.",
			`root.something_at = (this.created_at + 300).format_timestamp("2006-Jan-02 15:04:05", "UTC")`,
			`{"created_at":1597405526}`,
			`{"something_at":"2020-Aug-14 11:50:26"}`,
		),
	).IsBeta(true),
	true, formatTimestampMethod,
	ExpectBetweenNAndMArgs(0, 2),
	ExpectStringArg(0),
	ExpectStringArg(1),
)

func formatTimestampMethod(target Function, args ...interface{}) (Function, error) {
	layout := time.RFC3339
	if len(args) > 0 {
		layout = args[0].(string)
	}
	timezone := time.Local
	if len(args) > 1 {
		var err error
		if timezone, err = time.LoadLocation(args[1].(string)); err != nil {
			return nil, fmt.Errorf("failed to parse timezone location name: %w", err)
		}
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		u, err := IToInt(v)
		if err != nil {
			return nil, err
		}
		return time.Unix(u, 0).In(timezone).Format(layout), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"quote", "",
	).InCategory(
		MethodCategoryStrings,
		"Quotes a target string using escape sequences (`\t`, `\n`, `\xFF`, `\u0100`) for control characters and non-printable characters.",
		NewExampleSpec("",
			`root.quoted = this.thing.quote()`,
			`{"thing":"foo\nbar"}`,
			`{"quoted":"\"foo\\nbar\""}`,
		),
	),
	false, quoteMethod,
	ExpectNArgs(0),
)

func quoteMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strconv.Quote(t), nil
		case []byte:
			return strconv.Quote(string(t)), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"unquote", "",
	).InCategory(
		MethodCategoryStrings,
		"Unquotes a target string, expanding any escape sequences (`\t`, `\n`, `\xFF`, `\u0100`) for control characters and non-printable characters.",
		NewExampleSpec("",
			`root.unquoted = this.thing.unquote()`,
			`{"thing":"\"foo\\nbar\""}`,
			`{"unquoted":"foo\nbar"}`,
		),
	),
	false, unquoteMethod,
	ExpectNArgs(0),
)

func unquoteMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strconv.Unquote(t)
		case []byte:
			return strconv.Unquote(string(t))
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"replace", "",
	).InCategory(
		MethodCategoryStrings,
		"Replaces all occurrences of the first argument in a target string with the second argument.",
		NewExampleSpec("",
			`root.new_value = this.value.replace("foo","dog")`,
			`{"value":"The foo ate my homework"}`,
			`{"new_value":"The dog ate my homework"}`,
		),
	),
	true, replaceMethod,
	ExpectNArgs(2),
	ExpectStringArg(0),
	ExpectStringArg(1),
)

func replaceMethod(target Function, args ...interface{}) (Function, error) {
	match := args[0].(string)
	matchB := []byte(match)
	with := args[1].(string)
	withB := []byte(with)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return strings.ReplaceAll(t, match, with), nil
		case []byte:
			return bytes.ReplaceAll(t, matchB, withB), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"re_find_all", "",
	).InCategory(
		MethodCategoryRegexp,
		"Returns an array containing all successive matches of a regular expression in a string.",
		NewExampleSpec("",
			`root.matches = this.value.re_find_all("a.")`,
			`{"value":"paranormal"}`,
			`{"matches":["ar","an","al"]}`,
		),
	),
	true, regexpFindAllMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func regexpFindAllMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var result []interface{}
		switch t := v.(type) {
		case string:
			matches := re.FindAllString(t, -1)
			result = make([]interface{}, 0, len(matches))
			for _, str := range matches {
				result = append(result, str)
			}
		case []byte:
			matches := re.FindAll(t, -1)
			result = make([]interface{}, 0, len(matches))
			for _, str := range matches {
				result = append(result, string(str))
			}
		default:
			return nil, NewTypeError(v, ValueString)
		}
		return result, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"re_find_all_submatch", "",
	).InCategory(
		MethodCategoryRegexp,
		"Returns an array of arrays containing all successive matches of the regular expression in a string and the matches, if any, of its subexpressions.",
		NewExampleSpec("",
			`root.matches = this.value.re_find_all_submatch("a(x*)b")`,
			`{"value":"-axxb-ab-"}`,
			`{"matches":[["axxb","xx"],["ab",""]]}`,
		),
	),
	true, regexpFindAllSubmatchMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func regexpFindAllSubmatchMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var result []interface{}
		switch t := v.(type) {
		case string:
			groupMatches := re.FindAllStringSubmatch(t, -1)
			result = make([]interface{}, 0, len(groupMatches))
			for _, matches := range groupMatches {
				r := make([]interface{}, 0, len(matches))
				for _, str := range matches {
					r = append(r, str)
				}
				result = append(result, r)
			}
		case []byte:
			groupMatches := re.FindAllSubmatch(t, -1)
			result = make([]interface{}, 0, len(groupMatches))
			for _, matches := range groupMatches {
				r := make([]interface{}, 0, len(matches))
				for _, str := range matches {
					r = append(r, string(str))
				}
				result = append(result, r)
			}
		default:
			return nil, NewTypeError(v, ValueString)
		}
		return result, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"re_match", "",
	).InCategory(
		MethodCategoryRegexp,
		"Checks whether a regular expression matches against any part of a string and returns a boolean.",
		NewExampleSpec("",
			`root.matches = this.value.re_match("[0-9]")`,
			`{"value":"there are 10 puppies"}`,
			`{"matches":true}`,
			`{"value":"there are ten puppies"}`,
			`{"matches":false}`,
		),
	),
	true, regexpMatchMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func regexpMatchMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var result bool
		switch t := v.(type) {
		case string:
			result = re.MatchString(t)
		case []byte:
			result = re.Match(t)
		default:
			return nil, NewTypeError(v, ValueString)
		}
		return result, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"re_replace", "",
	).InCategory(
		MethodCategoryRegexp,
		"Replaces all occurrences of the argument regular expression in a string with a value. Inside the value $ signs are interpreted as submatch expansions, e.g. `$1` represents the text of the first submatch.",
		NewExampleSpec("",
			`root.new_value = this.value.re_replace("ADD ([0-9]+)","+($1)")`,
			`{"value":"foo ADD 70"}`,
			`{"new_value":"foo +(70)"}`,
		),
	),
	true, regexpReplaceMethod,
	ExpectNArgs(2),
	ExpectStringArg(0),
	ExpectStringArg(1),
)

func regexpReplaceMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	with := args[1].(string)
	withBytes := []byte(with)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		var result string
		switch t := v.(type) {
		case string:
			result = re.ReplaceAllString(t, with)
		case []byte:
			result = string(re.ReplaceAll(t, withBytes))
		default:
			return nil, NewTypeError(v, ValueString)
		}
		return result, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"split", "",
	).InCategory(
		MethodCategoryStrings,
		"Split a string value into an array of strings by splitting it on a string separator.",
		NewExampleSpec("",
			`root.new_value = this.value.split(",")`,
			`{"value":"foo,bar,baz"}`,
			`{"new_value":["foo","bar","baz"]}`,
		),
	),
	true, splitMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func splitMethod(target Function, args ...interface{}) (Function, error) {
	delim := args[0].(string)
	delimB := []byte(delim)
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			bits := strings.Split(t, delim)
			vals := make([]interface{}, 0, len(bits))
			for _, b := range bits {
				vals = append(vals, b)
			}
			return vals, nil
		case []byte:
			bits := bytes.Split(t, delimB)
			vals := make([]interface{}, 0, len(bits))
			for _, b := range bits {
				vals = append(vals, b)
			}
			return vals, nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"string", "",
	).InCategory(
		MethodCategoryCoercion,
		"Marshal a value into a string. If the value is already a string it is unchanged.",
		NewExampleSpec("",
			`root.nested_json = this.string()`,
			`{"foo":"bar"}`,
			`{"nested_json":"{\"foo\":\"bar\"}"}`,
		),
	),
	false, stringMethod,
	ExpectNArgs(0),
)

func stringMethod(target Function, _ ...interface{}) (Function, error) {
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		return IToString(v), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"strip_html", "",
	).InCategory(
		MethodCategoryStrings,
		"Attempts to remove all HTML tags from a target string.",
		NewExampleSpec("",
			`root.stripped = this.value.strip_html()`,
			`{"value":"<p>the plain <strong>old text</strong></p>"}`,
			`{"stripped":"the plain old text"}`,
		),
	),
	false, stripHTMLMethod,
	ExpectNArgs(0),
)

func stripHTMLMethod(target Function, _ ...interface{}) (Function, error) {
	p := bluemonday.NewPolicy()
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			return p.Sanitize(t), nil
		case []byte:
			return p.SanitizeBytes(t), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec(
		"trim", "",
	).InCategory(
		MethodCategoryStrings,
		"Remove all leading and trailing characters from a string that are contained within an argument cutset. If no arguments are provided then whitespace is removed.",
		NewExampleSpec("",
			`root.title = this.title.trim("!?")
root.description = this.description.trim()`,
			`{"description":"  something happened and its amazing! ","title":"!!!watch out!?"}`,
			`{"description":"something happened and its amazing!","title":"watch out"}`,
		),
	),
	true, trimMethod,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func trimMethod(target Function, args ...interface{}) (Function, error) {
	var cutset string
	if len(args) > 0 {
		cutset = args[0].(string)
	}
	return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
		switch t := v.(type) {
		case string:
			if len(cutset) == 0 {
				return strings.TrimSpace(t), nil
			}
			return strings.Trim(t, cutset), nil
		case []byte:
			if len(cutset) == 0 {
				return bytes.TrimSpace(t), nil
			}
			return bytes.Trim(t, cutset), nil
		}
		return nil, NewTypeError(v, ValueString)
	}), nil
}

//------------------------------------------------------------------------------
