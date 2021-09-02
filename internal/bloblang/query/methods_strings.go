package query

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/xml"
	"github.com/OneOfOne/xxhash"
	"github.com/itchyny/timefmt-go"
	"github.com/microcosm-cc/bluemonday"
	"github.com/tilinna/z85"
	"gopkg.in/yaml.v3"
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"bytes", "",
	).InCategory(
		MethodCategoryCoercion,
		"Marshal a value into a byte array. If the value is already a byte array it is unchanged.",
		NewExampleSpec("",
			`root.first_byte = this.name.bytes().index(0)`,
			`{"name":"foobar bazson"}`,
			`{"first_byte":102}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			return IToBytes(v), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return strings.Title(t), nil
			case []byte:
				return bytes.Title(t), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"encode", "",
	).InCategory(
		MethodCategoryEncoding,
		"Encodes a string or byte array target according to a chosen scheme and returns a string result. Available schemes are: `base64`, `base64url`, `hex`, `ascii85`.",
		// NOTE: z85 has been removed from the list until we can support
		// misaligned data automatically. It'll still be supported for backwards
		// compatibility, but given it behaves differently to `ascii85` I think
		// it's a poor user experience to expose it.
		NewExampleSpec("",
			`root.encoded = this.value.encode("hex")`,
			`{"value":"hello world"}`,
			`{"encoded":"68656c6c6f20776f726c64"}`,
		),
		NewExampleSpec("",
			`root.encoded = content().encode("ascii85")`,
			`this is totally unstructured data`,
			"{\"encoded\":\"FD,B0+DGm>FDl80Ci\\\"A>F`)8BEckl6F`M&(+Cno&@/\"}",
		),
	).Param(ParamString("scheme", "The encoding scheme to use.")),
	func(args *ParsedParams) (simpleMethod, error) {
		schemeStr, err := args.FieldString("scheme")
		if err != nil {
			return nil, err
		}

		var schemeFn func([]byte) (string, error)
		switch schemeStr {
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
				var buf bytes.Buffer
				e := ascii85.NewEncoder(&buf)
				if _, err := e.Write(b); err != nil {
					return "", err
				}
				if err := e.Close(); err != nil {
					return "", err
				}
				return buf.String(), nil
			}
		case "z85":
			schemeFn = func(b []byte) (string, error) {
				// TODO: Update this to support misaligned input data similar to the
				// ascii85 encoder.
				enc := make([]byte, z85.EncodedLen(len(b)))
				if _, err := z85.Encode(enc, b); err != nil {
					return "", err
				}
				return string(enc), nil
			}
		default:
			return nil, fmt.Errorf("unrecognized encoding type: %v", schemeStr)
		}

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"decode", "",
	).InCategory(
		MethodCategoryEncoding,
		"Decodes an encoded string target according to a chosen scheme and returns the result as a byte array. When mapping the result to a JSON field the value should be cast to a string using the method [`string`][methods.string], or encoded using the method [`encode`][methods.encode], otherwise it will be base64 encoded by default.\n\nAvailable schemes are: `base64`, `base64url`, `hex`, `ascii85`.",
		// NOTE: z85 has been removed from the list until we can support
		// misaligned data automatically. It'll still be supported for backwards
		// compatibility, but given it behaves differently to `ascii85` I think
		// it's a poor user experience to expose it.
		NewExampleSpec("",
			`root.decoded = this.value.decode("hex").string()`,
			`{"value":"68656c6c6f20776f726c64"}`,
			`{"decoded":"hello world"}`,
		),
		NewExampleSpec("",
			`root = this.encoded.decode("ascii85")`,
			"{\"encoded\":\"FD,B0+DGm>FDl80Ci\\\"A>F`)8BEckl6F`M&(+Cno&@/\"}",
			`this is totally unstructured data`,
		),
	).Param(ParamString("scheme", "The decoding scheme to use.")),
	func(args *ParsedParams) (simpleMethod, error) {
		schemeStr, err := args.FieldString("scheme")
		if err != nil {
			return nil, err
		}

		var schemeFn func([]byte) ([]byte, error)
		switch schemeStr {
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
				// TODO: Update this to support misaligned input data similar to the
				// ascii85 decoder.
				dec := make([]byte, z85.DecodedLen(len(b)))
				if _, err := z85.Decode(dec, b); err != nil {
					return nil, err
				}
				return dec, nil
			}
		default:
			return nil, fmt.Errorf("unrecognized encoding type: %v", schemeStr)
		}

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	).
		Param(ParamString("scheme", "The scheme to use for encryption, one of `ctr`, `ofb`, `cbc`.")).
		Param(ParamString("key", "A key to encrypt with.")).
		Param(ParamString("iv", "An initialization vector / nonce.")),
	func(args *ParsedParams) (simpleMethod, error) {
		schemeStr, err := args.FieldString("scheme")
		if err != nil {
			return nil, err
		}
		keyStr, err := args.FieldString("key")
		if err != nil {
			return nil, err
		}
		ivStr, err := args.FieldString("iv")
		if err != nil {
			return nil, err
		}

		key := []byte(keyStr)
		iv := []byte(ivStr)

		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}

		var schemeFn func([]byte) (string, error)
		switch schemeStr {
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
			return nil, fmt.Errorf("unrecognized encryption type: %v", schemeStr)
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	).
		Param(ParamString("scheme", "The scheme to use for decryption, one of `ctr`, `ofb`, `cbc`.")).
		Param(ParamString("key", "A key to decrypt with.")).
		Param(ParamString("iv", "An initialization vector / nonce.")),
	func(args *ParsedParams) (simpleMethod, error) {
		schemeStr, err := args.FieldString("scheme")
		if err != nil {
			return nil, err
		}
		keyStr, err := args.FieldString("key")
		if err != nil {
			return nil, err
		}
		ivStr, err := args.FieldString("iv")
		if err != nil {
			return nil, err
		}

		key := []byte(keyStr)
		iv := []byte(ivStr)

		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}

		var schemeFn func([]byte) ([]byte, error)
		switch schemeStr {
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
			return nil, fmt.Errorf("unrecognized decryption type: %v", schemeStr)
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return html.EscapeString(s), nil
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"index_of", "",
	).InCategory(
		MethodCategoryStrings,
		"Returns the starting index of the argument substring in a string target, or `-1` if the target doesn't contain the argument.",
		NewExampleSpec("",
			`root.index = this.thing.index_of("bar")`,
			`{"thing":"foobar"}`,
			`{"index":3}`,
		),
		NewExampleSpec("",
			`root.index = content().index_of("meow")`,
			`the cat meowed, the dog woofed`,
			`{"index":8}`,
		),
	),
	func(args ...interface{}) (simpleMethod, error) {
		substring := args[0].(string)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return int64(strings.Index(t, substring)), nil
			case []byte:
				return int64(bytes.Index(t, []byte(substring))), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return html.UnescapeString(s), nil
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return url.QueryEscape(s), nil
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return url.QueryUnescape(s)
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"filepath_join", "",
	).InCategory(
		MethodCategoryStrings,
		"Joins an array of path elements into a single file path. The separator depends on the operating system of the machine.",
		NewExampleSpec("",
			`root.path = this.path_elements.filepath_join()`,
			strings.ReplaceAll(`{"path_elements":["/foo/","bar.txt"]}`, "/", string(filepath.Separator)),
			strings.ReplaceAll(`{"path":"/foo/bar.txt"}`, "/", string(filepath.Separator)),
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			arr, ok := v.([]interface{})
			if !ok {
				return nil, NewTypeError(v, ValueArray)
			}
			strs := make([]string, 0, len(arr))
			for i, ele := range arr {
				str, err := IGetString(ele)
				if err != nil {
					return nil, fmt.Errorf("path element %v: %w", i, err)
				}
				strs = append(strs, str)
			}
			return filepath.Join(strs...), nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"filepath_split", "",
	).InCategory(
		MethodCategoryStrings,
		"Splits a file path immediately following the final Separator, separating it into a directory and file name component returned as a two element array of strings. If there is no Separator in the path, the first element will be empty and the second will contain the path. The separator depends on the operating system of the machine.",
		NewExampleSpec("",
			`root.path_sep = this.path.filepath_split()`,
			strings.ReplaceAll(`{"path":"/foo/bar.txt"}`, "/", string(filepath.Separator)),
			strings.ReplaceAll(`{"path_sep":["/foo/","bar.txt"]}`, "/", string(filepath.Separator)),
			`{"path":"baz.txt"}`,
			`{"path_sep":["","baz.txt"]}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			dir, file := filepath.Split(s)
			return []interface{}{dir, file}, nil
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return fmt.Sprintf(s, args...), nil
		}), nil
	},
	true,
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		prefix := args[0].(string)
		prefixB := []byte(prefix)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return strings.HasPrefix(t, prefix), nil
			case []byte:
				return bytes.HasPrefix(t, prefixB), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		prefix := args[0].(string)
		prefixB := []byte(prefix)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return strings.HasSuffix(t, prefix), nil
			case []byte:
				return bytes.HasSuffix(t, prefixB), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"hash", "",
	).InCategory(
		MethodCategoryEncoding,
		`
Hashes a string or byte array according to a chosen algorithm and returns the result as a byte array. When mapping the result to a JSON field the value should be cast to a string using the method `+"[`string`][methods.string], or encoded using the method [`encode`][methods.encode]"+`, otherwise it will be base64 encoded by default.

Available algorithms are: `+"`hmac_sha1`, `hmac_sha256`, `hmac_sha512`, `md5`, `sha1`, `sha256`, `sha512`, `xxhash64`"+`.

The following algorithms require a key, which is specified as a second argument: `+"`hmac_sha1`, `hmac_sha256`, `hmac_sha512`"+`.`,
		NewExampleSpec("",
			`root.h1 = this.value.hash("sha1").encode("hex")
root.h2 = this.value.hash("hmac_sha1","static-key").encode("hex")`,
			`{"value":"hello world"}`,
			`{"h1":"2aae6c35c94fcfb415dbe95f408b9ce91ee846ed","h2":"d87e5f068fa08fe90bb95bc7c8344cb809179d76"}`,
		),
	),
	func(args ...interface{}) (simpleMethod, error) {
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
		case "md5":
			hashFn = func(b []byte) ([]byte, error) {
				hasher := md5.New()
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
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectAtLeastOneArg(),
	oldParamsExpectStringArg(0),
	oldParamsExpectStringArg(1),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		var delim string
		if len(args) > 0 {
			delim = args[0].(string)
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectOneOrZeroArgs(),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return strings.ToUpper(t), nil
			case []byte:
				return bytes.ToUpper(t), nil
			default:
				return nil, NewTypeError(v, ValueString)
			}
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return strings.ToLower(t), nil
			case []byte:
				return bytes.ToLower(t), nil
			default:
				return nil, NewTypeError(v, ValueString)
			}
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	parseCSVMethod,
)

func parseCSVMethod(*ParsedParams) (simpleMethod, error) {
	return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
	}, nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"parse_xml", "",
	).InCategory(
		MethodCategoryParsing,
		`Attempts to parse a string as an XML document and returns a structured result, where elements appear as keys of an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen, `+"`-`"+`, to the attribute label.
- If the element is a simple element and has attributes, the element value is given the key `+"`#text`"+`.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.`,
		NewExampleSpec("",
			`root.doc = this.doc.parse_xml()`,
			`{"doc":"<root><title>This is a title</title><content>This is some content</content></root>"}`,
			`{"doc":{"root":{"content":"This is some content","title":"This is a title"}}}`,
		),
	).Beta(),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var xmlBytes []byte
			switch t := v.(type) {
			case string:
				xmlBytes = []byte(t)
			case []byte:
				xmlBytes = t
			default:
				return nil, NewTypeError(v, ValueString)
			}
			xmlObj, err := xml.ToMap(xmlBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value as XML: %w", err)
			}
			return xmlObj, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"parse_yaml", "",
	).InCategory(
		MethodCategoryParsing,
		"Attempts to parse a string as a single YAML document and returns the result.",
		NewExampleSpec("",
			`root.doc = this.doc.parse_yaml()`,
			`{"doc":"foo: bar"}`,
			`{"doc":{"foo":"bar"}}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var yamlBytes []byte
			switch t := v.(type) {
			case string:
				yamlBytes = []byte(t)
			case []byte:
				yamlBytes = t
			default:
				return nil, NewTypeError(v, ValueString)
			}
			var sObj interface{}
			if err := yaml.Unmarshal(yamlBytes, &sObj); err != nil {
				return nil, fmt.Errorf("failed to parse value as YAML: %w", err)
			}
			return sObj, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"format_yaml", "",
	).InCategory(
		MethodCategoryParsing,
		"Serializes a target value into a YAML byte array.",
		NewExampleSpec("",
			`root = this.doc.format_yaml()`,
			`{"doc":{"foo":"bar"}}`,
			`foo: bar
`,
		),
		NewExampleSpec("Use the `.string()` method in order to coerce the result into a string.",
			`root.doc = this.doc.format_yaml().string()`,
			`{"doc":{"foo":"bar"}}`,
			`{"doc":"foo: bar\n"}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			return yaml.Marshal(v)
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"parse_duration", "",
	).InCategory(
		MethodCategoryTime,
		`Attempts to parse a string as a duration and returns an integer of nanoseconds. A duration string is a possibly signed sequence of decimal numbers, each with an optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".`,
		NewExampleSpec("",
			`root.delay_for_ns = this.delay_for.parse_duration()`,
			`{"delay_for":"50us"}`,
			`{"delay_for_ns":50000}`,
		),
		NewExampleSpec("",
			`root.delay_for_s = this.delay_for.parse_duration() / 1000000000`,
			`{"delay_for":"2h"}`,
			`{"delay_for_s":7200}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			d, err := time.ParseDuration(s)
			if err != nil {
				return nil, err
			}
			return d.Nanoseconds(), nil
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewDeprecatedMethodSpec(
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
	func(args ...interface{}) (simpleMethod, error) {
		layout := time.RFC3339Nano
		if len(args) > 0 {
			layout = args[0].(string)
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectOneOrZeroArgs(),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"parse_timestamp", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to parse a string as a timestamp following a specified format and outputs a string following ISO 8601, which can then be fed into `format_timestamp`. The input format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value.",
		NewExampleSpec("",
			`root.doc.timestamp = this.doc.timestamp.parse_timestamp("2006-Jan-02")`,
			`{"doc":{"timestamp":"2020-Aug-14"}}`,
			`{"doc":{"timestamp":"2020-08-14T00:00:00Z"}}`,
		),
	).Beta(),
	func(args ...interface{}) (simpleMethod, error) {
		layout := args[0].(string)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
			return ut.Format(time.RFC3339Nano), nil
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"parse_timestamp_strptime", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to parse a string as a timestamp following a specified strptime-compatible format and outputs a string following ISO 8601, which can then be fed into `format_timestamp`.",
		NewExampleSpec(
			"The format consists of zero or more conversion specifiers and ordinary characters (except `%`). All ordinary characters are copied to the output string without modification. Each conversion specification begins with a `%` character followed by the character that determines the behaviour of the specifier. Please refer to [man 3 strptime](https://linux.die.net/man/3/strptime) for the list of format specifiers.",
			`root.doc.timestamp = this.doc.timestamp.parse_timestamp_strptime("%Y-%b-%d")`,
			`{"doc":{"timestamp":"2020-Aug-14"}}`,
			`{"doc":{"timestamp":"2020-08-14T00:00:00Z"}}`,
		),
	).Beta(),
	func(args ...interface{}) (simpleMethod, error) {
		layout := args[0].(string)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var str string
			switch t := v.(type) {
			case []byte:
				str = string(t)
			case string:
				str = t
			default:
				return nil, NewTypeError(v, ValueString)
			}
			ut, err := timefmt.Parse(str, layout)
			if err != nil {
				return nil, err
			}
			return ut.Format(time.RFC3339Nano), nil
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"reverse", "",
	).InCategory(
		MethodCategoryStrings,
		"Returns the target string in reverse order.",
		NewExampleSpec("",
			`root.reversed = this.thing.reverse()`,
			`{"thing":"backwards"}`,
			`{"reversed":"sdrawkcab"}`,
		),
		NewExampleSpec("",
			`root = content().reverse()`,
			`{"thing":"backwards"}`,
			`}"sdrawkcab":"gniht"{`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				runes := []rune(t)
				for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
					runes[i], runes[j] = runes[j], runes[i]
				}
				return string(runes), nil
			case []byte:
				result := make([]byte, len(t))
				for i, b := range t {
					result[len(t)-i-1] = b
				}
				return result, nil
			}

			return nil, NewTypeError(v, ValueString)
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"format_timestamp", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to format a timestamp value as a string according to a specified format, or ISO 8601 by default. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in ISO 8601 format.",
		NewExampleSpec("",
			`root.something_at = (this.created_at + 300).format_timestamp()`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-08-14T11:50:26.371Z"}`,
		),
		NewExampleSpec(
			"An optional string argument can be used in order to specify the output format of the timestamp. The format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value.",
			`root.something_at = (this.created_at + 300).format_timestamp("2006-Jan-02 15:04:05")`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-Aug-14 11:50:26"}`,
		),
		NewExampleSpec(
			"A second optional string argument can also be used in order to specify a timezone, otherwise the timezone of the input string is used, or in the case of unix timestamps the local timezone is used.",
			`root.something_at = this.created_at.format_timestamp(format: "2006-Jan-02 15:04:05", tz: "UTC")`,

			`{"created_at":1597405526}`,
			`{"something_at":"2020-Aug-14 11:45:26"}`,

			`{"created_at":"2020-08-14T11:50:26.371Z"}`,
			`{"something_at":"2020-Aug-14 11:50:26"}`,
		),
		NewExampleSpec(
			"And `format_timestamp` supports up to nanosecond precision with floating point timestamp values.",
			`root.something_at = this.created_at.format_timestamp("2006-Jan-02 15:04:05.999999", "UTC")`,

			`{"created_at":1597405526.123456}`,
			`{"something_at":"2020-Aug-14 11:45:26.123456"}`,

			`{"created_at":"2020-08-14T11:50:26.371Z"}`,
			`{"something_at":"2020-Aug-14 11:50:26.371"}`,
		),
	).Beta().
		Param(ParamString("format", "The output format to use.").Default(time.RFC3339Nano)).
		Param(ParamString("tz", "An optional timezone to use, otherwise the timezone of the input string is used, or in the case of unix timestamps the local timezone is used.").Optional()),
	func(args *ParsedParams) (simpleMethod, error) {
		layout, err := args.FieldString("format")
		if err != nil {
			return nil, err
		}
		var timezone *time.Location
		tzOpt, err := args.FieldOptionalString("tz")
		if err != nil {
			return nil, err
		}
		if tzOpt != nil {
			if timezone, err = time.LoadLocation(*tzOpt); err != nil {
				return nil, fmt.Errorf("failed to parse timezone location name: %w", err)
			}
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			target, err := IGetTimestamp(v)
			if err != nil {
				return nil, err
			}
			if timezone != nil {
				target = target.In(timezone)
			}
			return target.Format(layout), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"format_timestamp_strftime", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to format a timestamp value as a string according to a specified strftime-compatible format. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in ISO 8601 format.",
		NewExampleSpec(
			"The format consists of zero or more conversion specifiers and ordinary characters (except `%`). All ordinary characters are copied to the output string without modification. Each conversion specification begins with `%` character followed by the character that determines the behaviour of the specifier. Please refer to [man 3 strftime](https://linux.die.net/man/3/strftime) for the list of format specifiers.",
			`root.something_at = (this.created_at + 300).format_timestamp_strftime("%Y-%b-%d %H:%M:%S")`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-Aug-14 11:50:26"}`,
		),
		NewExampleSpec(
			"A second optional string argument can also be used in order to specify a timezone, otherwise the timezone of the input string is used, or in the case of unix timestamps the local timezone is used.",
			`root.something_at = this.created_at.format_timestamp_strftime("%Y-%b-%d %H:%M:%S", "UTC")`,

			`{"created_at":1597405526}`,
			`{"something_at":"2020-Aug-14 11:45:26"}`,

			`{"created_at":"2020-08-14T11:50:26.371Z"}`,
			`{"something_at":"2020-Aug-14 11:50:26"}`,
		),
	).Beta(),
	func(args ...interface{}) (simpleMethod, error) {
		layout := args[0].(string)
		var timezone *time.Location
		if len(args) > 1 {
			var err error
			if timezone, err = time.LoadLocation(args[1].(string)); err != nil {
				return nil, fmt.Errorf("failed to parse timezone location name: %w", err)
			}
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			target, err := IGetTimestamp(v)
			if err != nil {
				return nil, err
			}
			if timezone != nil {
				target = target.In(timezone)
			}
			return timefmt.Format(target, layout), nil
		}, nil
	},
	true,
	oldParamsExpectBetweenNAndMArgs(1, 2),
	oldParamsExpectStringArg(0),
	oldParamsExpectStringArg(1),
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"format_timestamp_unix", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to format a timestamp value as a unix timestamp. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in ISO 8601 format. The [`parse_timestamp`](#parse_timestamp) method can be used in order to parse different timestamp formats.",
		NewExampleSpec("",
			`root.created_at_unix = this.created_at.format_timestamp_unix()`,
			`{"created_at":"2009-11-10T23:00:00Z"}`,
			`{"created_at_unix":1257894000}`,
		),
	).Beta(),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			target, err := IGetTimestamp(v)
			if err != nil {
				return nil, err
			}

			return target.Unix(), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"format_timestamp_unix_nano", "",
	).InCategory(
		MethodCategoryTime,
		"Attempts to format a timestamp value as a unix timestamp with nanosecond precision. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in ISO 8601 format. The [`parse_timestamp`](#parse_timestamp) method can be used in order to parse different timestamp formats.",
		NewExampleSpec("",
			`root.created_at_unix = this.created_at.format_timestamp_unix_nano()`,
			`{"created_at":"2009-11-10T23:00:00Z"}`,
			`{"created_at_unix":1257894000000000000}`,
		),
	).Beta(),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			target, err := IGetTimestamp(v)
			if err != nil {
				return nil, err
			}

			return target.UnixNano(), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"quote", "",
	).InCategory(
		MethodCategoryStrings,
		"Quotes a target string using escape sequences (`\\t`, `\\n`, `\\xFF`, `\\u0100`) for control characters and non-printable characters.",
		NewExampleSpec("",
			`root.quoted = this.thing.quote()`,
			`{"thing":"foo\nbar"}`,
			`{"quoted":"\"foo\\nbar\""}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return strconv.Quote(s), nil
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"unquote", "",
	).InCategory(
		MethodCategoryStrings,
		"Unquotes a target string, expanding any escape sequences (`\\t`, `\\n`, `\\xFF`, `\\u0100`) for control characters and non-printable characters.",
		NewExampleSpec("",
			`root.unquoted = this.thing.unquote()`,
			`{"thing":"\"foo\\nbar\""}`,
			`{"unquoted":"foo\nbar"}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return stringMethod(func(s string) (interface{}, error) {
			return strconv.Unquote(s)
		}), nil
	},
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		match := args[0].(string)
		matchB := []byte(match)
		with := args[1].(string)
		withB := []byte(with)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return strings.ReplaceAll(t, match, with), nil
			case []byte:
				return bytes.ReplaceAll(t, matchB, withB), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectNArgs(2),
	oldParamsExpectStringArg(0),
	oldParamsExpectStringArg(1),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"replace_many", "",
	).InCategory(
		MethodCategoryStrings,
		"For each pair of strings in an argument array, replaces all occurrences of the first item of the pair with the second. This is a more compact way of chaining a series of `replace` methods.",
		NewExampleSpec("",
			`root.new_value = this.value.replace_many([
  "<b>", "&lt;b&gt;",
  "</b>", "&lt;/b&gt;",
  "<i>", "&lt;i&gt;",
  "</i>", "&lt;/i&gt;",
])`,
			`{"value":"<i>Hello</i> <b>World</b>"}`,
			`{"new_value":"&lt;i&gt;Hello&lt;/i&gt; &lt;b&gt;World&lt;/b&gt;"}`,
		),
	),
	func(args ...interface{}) (simpleMethod, error) {
		items, ok := args[0].([]interface{})
		if !ok {
			return nil, NewTypeError(args[0], ValueArray)
		}
		if len(items)%2 != 0 {
			return nil, fmt.Errorf("invalid arg, replacements should be in pairs and must therefore be even: %v", items)
		}

		var replacePairs [][2]string
		var replacePairsBytes [][2][]byte

		for i := 0; i < len(items); i += 2 {
			from, err := IGetString(items[i])
			if err != nil {
				return nil, fmt.Errorf("invalid replacement value at index %v: %w", i, err)
			}
			to, err := IGetString(items[i+1])
			if err != nil {
				return nil, fmt.Errorf("invalid replacement value at index %v: %w", i+1, err)
			}
			replacePairs = append(replacePairs, [2]string{from, to})
			replacePairsBytes = append(replacePairsBytes, [2][]byte{[]byte(from), []byte(to)})
		}

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				for _, pair := range replacePairs {
					t = strings.ReplaceAll(t, pair[0], pair[1])
				}
				return t, nil
			case []byte:
				for _, pair := range replacePairsBytes {
					t = bytes.ReplaceAll(t, pair[0], pair[1])
				}
				return t, nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		re, err := regexp.Compile(args[0].(string))
		if err != nil {
			return nil, err
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		re, err := regexp.Compile(args[0].(string))
		if err != nil {
			return nil, err
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"re_find_object", "",
	).InCategory(
		MethodCategoryRegexp,
		"Returns an object containing the first match of the regular expression and the matches of its subexpressions. The key of each match value is the name of the group when specified, otherwise it is the index of the matching group, starting with the expression as a whole at 0.",
		NewExampleSpec("",
			`root.matches = this.value.re_find_object("a(?P<foo>x*)b")`,
			`{"value":"-axxb-ab-"}`,
			`{"matches":{"0":"axxb","foo":"xx"}}`,
		),
		NewExampleSpec("",
			`root.matches = this.value.re_find_object("(?P<key>\\w+):\\s+(?P<value>\\w+)")`,
			`{"value":"option1: value1"}`,
			`{"matches":{"0":"option1: value1","key":"option1","value":"value1"}}`,
		),
	),
	func(args ...interface{}) (simpleMethod, error) {
		re, err := regexp.Compile(args[0].(string))
		if err != nil {
			return nil, err
		}
		groups := re.SubexpNames()
		for i, k := range groups {
			if k == "" {
				groups[i] = fmt.Sprintf("%v", i)
			}
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			result := make(map[string]interface{}, len(groups))
			switch t := v.(type) {
			case string:
				groupMatches := re.FindStringSubmatch(t)
				for i, match := range groupMatches {
					key := groups[i]
					result[key] = match
				}
			case []byte:
				groupMatches := re.FindSubmatch(t)
				for i, match := range groupMatches {
					key := groups[i]
					result[key] = match
				}
			default:
				return nil, NewTypeError(v, ValueString)
			}
			return result, nil
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
	NewMethodSpec(
		"re_find_all_object", "",
	).InCategory(
		MethodCategoryRegexp,
		"Returns an array of objects containing all matches of the regular expression and the matches of its subexpressions. The key of each match value is the name of the group when specified, otherwise it is the index of the matching group, starting with the expression as a whole at 0.",
		NewExampleSpec("",
			`root.matches = this.value.re_find_all_object("a(?P<foo>x*)b")`,
			`{"value":"-axxb-ab-"}`,
			`{"matches":[{"0":"axxb","foo":"xx"},{"0":"ab","foo":""}]}`,
		),
		NewExampleSpec("",
			`root.matches = this.value.re_find_all_object("(?m)(?P<key>\\w+):\\s+(?P<value>\\w+)$")`,
			`{"value":"option1: value1\noption2: value2\noption3: value3"}`,
			`{"matches":[{"0":"option1: value1","key":"option1","value":"value1"},{"0":"option2: value2","key":"option2","value":"value2"},{"0":"option3: value3","key":"option3","value":"value3"}]}`,
		),
	),
	func(args ...interface{}) (simpleMethod, error) {
		re, err := regexp.Compile(args[0].(string))
		if err != nil {
			return nil, err
		}
		groups := re.SubexpNames()
		for i, k := range groups {
			if k == "" {
				groups[i] = fmt.Sprintf("%v", i)
			}
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var result []interface{}
			switch t := v.(type) {
			case string:
				reMatches := re.FindAllStringSubmatch(t, -1)
				result = make([]interface{}, 0, len(reMatches))
				for _, matches := range reMatches {
					obj := make(map[string]interface{}, len(groups))
					for i, match := range matches {
						key := groups[i]
						obj[key] = match
					}
					result = append(result, obj)
				}
			case []byte:
				reMatches := re.FindAllSubmatch(t, -1)
				result = make([]interface{}, 0, len(reMatches))
				for _, matches := range reMatches {
					obj := make(map[string]interface{}, len(groups))
					for i, match := range matches {
						key := groups[i]
						obj[key] = match
					}
					result = append(result, obj)
				}
			default:
				return nil, NewTypeError(v, ValueString)
			}
			return result, nil
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		re, err := regexp.Compile(args[0].(string))
		if err != nil {
			return nil, err
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		re, err := regexp.Compile(args[0].(string))
		if err != nil {
			return nil, err
		}
		with := args[1].(string)
		withBytes := []byte(with)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectNArgs(2),
	oldParamsExpectStringArg(0),
	oldParamsExpectStringArg(1),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		delim := args[0].(string)
		delimB := []byte(delim)
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
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
		}, nil
	},
	true,
	oldParamsExpectNArgs(1),
	oldParamsExpectStringArg(0),
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
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
		NewExampleSpec("",
			`root.id = this.id.string()`,
			`{"id":228930314431312345}`,
			`{"id":"228930314431312345"}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			return IToString(v), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
		NewExampleSpec("It's also possible to provide an explicit list of element types to preserve in the output.",
			`root.stripped = this.value.strip_html(["article"])`,
			`{"value":"<article><p>the plain <strong>old text</strong></p></article>"}`,
			`{"stripped":"<article>the plain old text</article>"}`,
		),
	),
	func(args ...interface{}) (simpleMethod, error) {
		p := bluemonday.NewPolicy()
		if len(args) > 0 {
			tags, ok := args[0].([]interface{})
			if !ok {
				return nil, NewTypeError(args[0], ValueArray)
			}
			tagStrs := make([]string, len(tags))
			for i, ele := range tags {
				if tagStrs[i], ok = ele.(string); !ok {
					return nil, fmt.Errorf("invalid arg at index %v: %w", i, NewTypeError(ele, ValueString))
				}
			}
			p = p.AllowElements(tagStrs...)
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				return p.Sanitize(t), nil
			case []byte:
				return p.SanitizeBytes(t), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectOneOrZeroArgs(),
)

//------------------------------------------------------------------------------

var _ = registerOldParamsSimpleMethod(
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
	func(args ...interface{}) (simpleMethod, error) {
		var cutset string
		if len(args) > 0 {
			cutset = args[0].(string)
		}
		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			switch t := v.(type) {
			case string:
				if cutset == "" {
					return strings.TrimSpace(t), nil
				}
				return strings.Trim(t, cutset), nil
			case []byte:
				if cutset == "" {
					return bytes.TrimSpace(t), nil
				}
				return bytes.Trim(t, cutset), nil
			}
			return nil, NewTypeError(v, ValueString)
		}, nil
	},
	true,
	oldParamsExpectOneOrZeroArgs(),
	oldParamsExpectStringArg(0),
)
