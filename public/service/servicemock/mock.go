package servicemock

// Type ''go generate'' to build mock using mockery
//go:generate go install -v github.com/vektra/mockery/v2/...@latest
//go:generate mockery --dir=.. --name=Cache --case snake --outpkg servicemock --output .
