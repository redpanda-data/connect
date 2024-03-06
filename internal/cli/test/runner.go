package test

import (
	"errors"
	"os"

	"github.com/benthosdev/benthos/v4/internal/log"
)

type RunnerKind string

var (
	LegacyRunner RunnerKind = "legacy"
	NeoRunner    RunnerKind = "neo"
)

var ErrInvalidRunner = errors.New("invalid runner kind")

func GetRunner(kind RunnerKind) (Runner, error) {
	switch kind {
	case LegacyRunner:
		return &legacyRunner{}, nil
	case NeoRunner:
		return &neoRunner{w: os.Stdout, ew: os.Stderr}, nil
	}
	return nil, ErrInvalidRunner

}

type RunConfig struct {
	Paths         []string
	TestSuffix    string
	Lint          bool
	Logger        log.Modular
	ResourcePaths []string
	Format        string
}

type Runner interface {
	Run(config RunConfig) bool
}
