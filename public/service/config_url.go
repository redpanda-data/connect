package service

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewURLField defines a new config field that describes a string that should
// contain a valid URL. It is then possible to extract either a string or a
// *url.URL from the resulting parsed config with the methods FieldString or
// FieldURL respectively.
func NewURLField(name string) *ConfigField {
	tf := docs.FieldURL(name, "")
	return &ConfigField{field: tf}
}

// FieldURL accesses a field from a parsed config that was defined with
// NewURLField and returns either a *url.URL or an error if the string was
// invalid.
func (p *ParsedConfig) FieldURL(path ...string) (*url.URL, error) {
	v, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	str, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a string, got %T", strings.Join(path, "."), v)
	}

	u, err := url.Parse(str)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url field '%v': %v", strings.Join(path, "."), err)
	}

	return u, nil
}

// NewURLListField defines a new config field that describes an array of strings
// that should contain only valid URLs. It is then possible to extract either a
// string slice or a slice of *url.URL from the resulting parsed config with the
// methods FieldStringArray or FieldURLArray respectively.
func NewURLListField(name string) *ConfigField {
	tf := docs.FieldURL(name, "").Array()
	return &ConfigField{field: tf}
}

func urlsFromStr(str string) (urls []*url.URL, err error) {
	for _, s := range strings.Split(str, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		var u *url.URL
		if u, err = url.Parse(s); err != nil {
			return
		}
		urls = append(urls, u)
	}
	return
}

// FieldURLList accesses a field from a parsed config that was defined with
// NewURLListField and returns either a []*url.URL or an error if one or more
// strings were invalid.
func (p *ParsedConfig) FieldURLList(path ...string) ([]*url.URL, error) {
	v, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	iList, ok := v.([]any)
	if !ok {
		switch t := v.(type) {
		case []*url.URL:
			return t, nil
		case []string:
			uList := make([]*url.URL, 0, len(t))
			for i, s := range t {
				urls, err := urlsFromStr(s)
				if err != nil {
					return nil, fmt.Errorf("failed to parse url field '%v': %v", strings.Join(path, ".")+"."+strconv.Itoa(i), err)
				}
				uList = append(uList, urls...)
			}
			return uList, nil
		default:
			return nil, fmt.Errorf("expected field '%v' to be a string list, got %T", p.i.FullDotPath(path...), v)
		}
	}

	uList := make([]*url.URL, 0, len(iList))
	for i, ev := range iList {
		switch t := ev.(type) {
		case *url.URL:
			uList[i] = t
		case string:
			urls, err := urlsFromStr(t)
			if err != nil {
				return nil, fmt.Errorf("failed to parse url field '%v': %v", strings.Join(path, ".")+"."+strconv.Itoa(i), err)
			}
			uList = append(uList, urls...)
		default:
			return nil, fmt.Errorf("expected field '%v' to be a string, got %T", strings.Join(path, ".")+"."+strconv.Itoa(i), v)
		}
	}

	return uList, nil
}
