package ifs

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

type testFS struct {
	fstest.MapFS
}

func (t testFS) MkdirAll(path string, perm fs.FileMode) error {
	return errors.New("not implemented")
}

func (t testFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return nil, errors.New("not implemented")
}

func (t testFS) Remove(name string) error {
	return errors.New("not implemented")
}

func TestOSAccess(t *testing.T) {
	var fs FS = testFS{}

	require.False(t, IsOS(fs))

	fs = OS()

	require.True(t, IsOS(fs))
}
