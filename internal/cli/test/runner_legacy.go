package test

type legacyRunner struct {
}

func (l *legacyRunner) Run(config RunConfig) bool {
	return RunAll(config.Paths, config.TestSuffix, config.Lint, config.Logger, config.ResourcePaths)
}
