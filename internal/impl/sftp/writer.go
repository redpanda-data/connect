package sftp

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

type codecSuffixFn func(data []byte) ([]byte, bool)

func codecGetWriter(codec string) (sFn codecSuffixFn, appendMode bool, err error) {
	switch codec {
	case "all-bytes":
		return func(data []byte) ([]byte, bool) { return nil, false }, false, nil
	case "append":
		return customDelimSuffixFn(""), true, nil
	case "lines":
		return customDelimSuffixFn("\n"), true, nil
	}
	if strings.HasPrefix(codec, "delim:") {
		by := strings.TrimPrefix(codec, "delim:")
		if by == "" {
			return nil, false, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return customDelimSuffixFn(by), true, nil
	}
	return nil, false, fmt.Errorf("codec was not recognised: %v", codec)
}

func customDelimSuffixFn(suffix string) codecSuffixFn {
	suffixB := []byte(suffix)
	return func(data []byte) ([]byte, bool) {
		if len(suffixB) == 0 {
			return nil, false
		}
		if !bytes.HasSuffix(data, suffixB) {
			return suffixB, true
		}
		return nil, false
	}
}
