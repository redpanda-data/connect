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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"io/ioutil"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/OneOfOne/xxhash"
	"github.com/microcosm-cc/bluemonday"
	"github.com/tilinna/z85"
)

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"capitalize", false, capitalizeMethod,
	ExpectNArgs(0),
)

func capitalizeMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"decode", true, decodeMethod,
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
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res []byte
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
	"encode", true, encodeMethod,
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
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res string
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
	"decryptaes", true, decryptAESMethod,
	ExpectNArgs(3),
	ExpectAllStringArgs(),
)

func decryptAESMethod(target Function, args ...interface{}) (Function, error) {
	var schemeFn func([]byte) ([]byte, error)
	switch args[0].(string) {
	case "ctr":
		schemeFn = func(b []byte) ([]byte, error) {
			key := []byte(args[1].(string))
			ciphertext := b
			iv := []byte(args[2].(string))

			block, err := aes.NewCipher(key)
			if err != nil {
				return nil, err
			}
			plaintext := make([]byte, len(ciphertext))

			stream := cipher.NewCTR(block, iv)
			stream.XORKeyStream(plaintext, ciphertext)

			return plaintext, nil
		}

	default:
		return nil, fmt.Errorf("unrecognized decryption type: %v", args[0])
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res []byte
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
	"encryptaes", true, encryptAESMethod,
	ExpectNArgs(3),
	ExpectAllStringArgs(),
)

func encryptAESMethod(target Function, args ...interface{}) (Function, error) {
	var schemeFn func([]byte) (string, error)
	switch args[0].(string) {
	case "ctr":
		schemeFn = func(b []byte) (string, error) {
			key := []byte(args[1].(string))
			plaintext := b
			iv := []byte(args[2].(string))

			block, err := aes.NewCipher(key)
			if err != nil {
				return "", err
			}
			ciphertext := make([]byte, len(plaintext))

			stream := cipher.NewCTR(block, iv)
			stream.XORKeyStream(ciphertext, plaintext)

			return string(ciphertext), nil
		}
	default:
		return nil, fmt.Errorf("unrecognized encryption type: %v", args[0])
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res string
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
	"escape_html", false, escapeHTMLMethod,
	ExpectNArgs(0),
)

func escapeHTMLMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res string
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
	"escape_url_query", false, escapeURLQueryMethod,
	ExpectNArgs(0),
)

func escapeURLQueryMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res string
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
	"format", true, formatMethod,
)

func formatMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"has_prefix", true, hasPrefixMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func hasPrefixMethod(target Function, args ...interface{}) (Function, error) {
	prefix := args[0].(string)
	prefixB := []byte(prefix)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"has_suffix", true, hasSuffixMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func hasSuffixMethod(target Function, args ...interface{}) (Function, error) {
	prefix := args[0].(string)
	prefixB := []byte(prefix)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"hash", true, hashMethod,
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

	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res []byte
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
	"join", true, joinMethod,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func joinMethod(target Function, args ...interface{}) (Function, error) {
	var delim string
	if len(args) > 0 {
		delim = args[0].(string)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

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
	"lowercase", false, lowercaseMethod,
	ExpectNArgs(0),
)

func lowercaseMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
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
	"parse_json", false, parseJSONMethod,
	ExpectNArgs(0),
)

func parseJSONMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
		if err = json.Unmarshal(jsonBytes, &jObj); err != nil {
			return nil, fmt.Errorf("failed to parse value as JSON: %w", err)
		}
		return jObj, nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"quote", false, quoteMethod,
	ExpectNArgs(0),
)

func quoteMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"replace", true, replaceMethod,
	ExpectNArgs(2),
	ExpectStringArg(0),
	ExpectStringArg(1),
)

func replaceMethod(target Function, args ...interface{}) (Function, error) {
	match := args[0].(string)
	matchB := []byte(match)
	with := args[1].(string)
	withB := []byte(with)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"re_find_all", true, regexpFindAllMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func regexpFindAllMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"re_find_all_submatch", true, regexpFindAllSubmatchMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func regexpFindAllSubmatchMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"re_match", true, regexpMatchMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func regexpMatchMethod(target Function, args ...interface{}) (Function, error) {
	re, err := regexp.Compile(args[0].(string))
	if err != nil {
		return nil, err
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"re_replace", true, regexpReplaceMethod,
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
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"split", true, splitMethod,
	ExpectNArgs(1),
	ExpectStringArg(0),
)

func splitMethod(target Function, args ...interface{}) (Function, error) {
	delim := args[0].(string)
	delimB := []byte(delim)
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"string", false, stringMethod,
	ExpectNArgs(0),
)

func stringMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
		return IToString(v), nil
	}), nil
}

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	"strip_html", false, stripHTMLMethod,
	ExpectNArgs(0),
)

func stripHTMLMethod(target Function, _ ...interface{}) (Function, error) {
	p := bluemonday.NewPolicy()
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"trim", true, trimMethod,
	ExpectOneOrZeroArgs(),
	ExpectStringArg(0),
)

func trimMethod(target Function, args ...interface{}) (Function, error) {
	var cutset string
	if len(args) > 0 {
		cutset = args[0].(string)
	}
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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

var _ = RegisterMethod(
	"unescape_html", false, unescapeHTMLMethod,
	ExpectNArgs(0),
)

func unescapeHTMLMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res string
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
	"unescape_url_query", false, unescapeURLQueryMethod,
	ExpectNArgs(0),
)

func unescapeURLQueryMethod(target Function, args ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		var res string
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
	"unquote", false, unquoteMethod,
	ExpectNArgs(0),
)

func unquoteMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
	"uppercase", false, uppercaseMethod,
	ExpectNArgs(0),
)

func uppercaseMethod(target Function, _ ...interface{}) (Function, error) {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, &ErrRecoverable{
				Recovered: "",
				Err:       err,
			}
		}
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
