.PHONY: all deps docker docker-cgo clean test test-race test-integration fmt lint install

TAGS ?=

GOMAXPROCS         ?= 1
INSTALL_DIR        ?= $(GOPATH)/bin
WEBSITE_DIR        ?= ./docs/modules
DEST_DIR           ?= ./target
PATHINSTBIN        = $(DEST_DIR)/bin
DOCKER_IMAGE       ?= docker.redpanda.com/redpandadata/connect

VERSION   := $(shell git describe --tags 2> /dev/null || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)
VER_MAJOR := $(shell echo $(VER_CUT) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VER_CUT) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VER_CUT) | cut -f3 -d.)
VER_RC    := $(shell echo $(VER_PATCH) | cut -f2 -d-)
DATE      := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

VER_FLAGS = -X main.Version=$(VERSION) -X main.DateBuilt=$(DATE)

LD_FLAGS   ?= -w -s
GO_FLAGS   ?=
DOCS_FLAGS ?=

APPS = redpanda-connect redpanda-connect-cloud redpanda-connect-community
all: $(APPS)

install: $(APPS)
	@install -d $(INSTALL_DIR)
	@rm -f $(INSTALL_DIR)/redpanda-connect
	@cp $(PATHINSTBIN)/* $(INSTALL_DIR)/

deps:
	@go mod tidy

SOURCE_FILES = $(shell find internal public cmd -type f)
TEMPLATE_FILES = $(shell find internal/impl -type f -name "template_*.yaml")

$(PATHINSTBIN)/%: $(SOURCE_FILES)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

docker-tags:
	@echo "latest,$(VER_CUT),$(VER_MAJOR).$(VER_MINOR),$(VER_MAJOR)" > .tags

docker-rc-tags:
	@echo "latest,$(VER_CUT),$(VER_MAJOR)-$(VER_RC)" > .tags

docker-cgo-tags:
	@echo "latest-cgo,$(VER_CUT)-cgo,$(VER_MAJOR).$(VER_MINOR)-cgo,$(VER_MAJOR)-cgo" > .tags

docker:
	@docker build -f ./resources/docker/Dockerfile . -t $(DOCKER_IMAGE):$(VER_CUT)
	@docker tag $(DOCKER_IMAGE):$(VER_CUT) $(DOCKER_IMAGE):latest

docker-cloud:
	@docker build -f ./resources/docker/Dockerfile.cloud . -t $(DOCKER_IMAGE):$(VER_CUT)-cloud
	@docker tag $(DOCKER_IMAGE):$(VER_CUT)-cloud $(DOCKER_IMAGE):latest-cloud

docker-cgo:
	@docker build -f ./resources/docker/Dockerfile.cgo . -t $(DOCKER_IMAGE):$(VER_CUT)-cgo
	@docker tag $(DOCKER_IMAGE):$(VER_CUT)-cgo $(DOCKER_IMAGE):latest-cgo

fmt:
	@go list -f {{.Dir}} ./... | xargs -I{} gofmt -w -s {}
	@go list -f {{.Dir}} ./... | xargs -I{} goimports -w -local github.com/redpanda-data/connect/v4 {}
	@go mod tidy

lint:
	@go vet $(GO_FLAGS) ./...
	@golangci-lint -j $(GOMAXPROCS) run --timeout 5m cmd/... internal/... public/...

test: $(APPS)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -timeout 3m ./...
	@$(PATHINSTBIN)/redpanda-connect template lint $(TEMPLATE_FILES)
	@$(PATHINSTBIN)/redpanda-connect test ./config/test/...

test-race: $(APPS)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -timeout 3m -race ./...

test-integration:
	$(warning WARNING! Running the integration tests in their entirety consumes a huge amount of computing resources and is likely to time out on most machines. It's recommended that you instead run the integration suite for connectors you are working selectively with `go test -run 'TestIntegration/kafka' ./...` and so on.)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -run "^Test.*Integration.*$$" -timeout 5m ./...

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist

docs: $(APPS) $(TOOLS)
	@go run -tags "$(TAGS)" ./cmd/tools/docs_gen
	@$(PATHINSTBIN)/redpanda-connect lint --deprecated "./config/examples/*.yaml" \
		"$(WEBSITE_DIR)/**/*.md"
	@$(PATHINSTBIN)/redpanda-connect template lint "./config/template_examples/*.yaml"
