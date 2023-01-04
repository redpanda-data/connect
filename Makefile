.PHONY: all serverless deps docker docker-cgo clean docs test test-race test-integration fmt lint install deploy-docs

TAGS ?=

GOMAXPROCS         ?= 1
INSTALL_DIR        ?= $(GOPATH)/bin
WEBSITE_DIR        ?= ./website
DEST_DIR           ?= ./target
PATHINSTBIN        = $(DEST_DIR)/bin
PATHINSTTOOLS      = $(DEST_DIR)/tools
PATHINSTSERVERLESS = $(DEST_DIR)/serverless
PATHINSTDOCKER     = $(DEST_DIR)/docker
DOCKER_IMAGE       ?= jeffail/benthos

VERSION   := $(shell git describe --tags || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)
VER_MAJOR := $(shell echo $(VER_CUT) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VER_CUT) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VER_CUT) | cut -f3 -d.)
VER_RC    := $(shell echo $(VER_PATCH) | cut -f2 -d-)
DATE      := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

VER_FLAGS = -X github.com/benthosdev/benthos/v4/internal/cli.Version=$(VERSION) \
	-X github.com/benthosdev/benthos/v4/internal/cli.DateBuilt=$(DATE)

LD_FLAGS   ?= -w -s
GO_FLAGS   ?=
DOCS_FLAGS ?=

APPS = benthos
all: $(APPS)

install: $(APPS)
	@install -d $(INSTALL_DIR)
	@rm -f $(INSTALL_DIR)/benthos
	@cp $(PATHINSTBIN)/* $(INSTALL_DIR)/

deps:
	@go mod tidy

SOURCE_FILES = $(shell find internal public cmd -type f)
TEMPLATE_FILES = $(shell find template -path template/test -prune -o -type f -name "*.yaml")

$(PATHINSTBIN)/%: $(SOURCE_FILES) $(TEMPLATE_FILES)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

TOOLS = benthos_docs_gen
tools: $(TOOLS)

$(PATHINSTTOOLS)/%: $(SOURCE_FILES) $(TEMPLATE_FILES)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/tools/$*

$(TOOLS): %: $(PATHINSTTOOLS)/%

SERVERLESS = benthos-lambda
serverless: $(SERVERLESS)

$(PATHINSTSERVERLESS)/%: $(SOURCE_FILES) $(TEMPLATE_FILES)
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/serverless/$*
	@zip -m -j $@.zip $@

$(SERVERLESS): %: $(PATHINSTSERVERLESS)/%

docker-tags:
	@echo "latest,$(VER_CUT),$(VER_MAJOR).$(VER_MINOR),$(VER_MAJOR)" > .tags

docker-rc-tags:
	@echo "latest,$(VER_CUT),$(VER_MAJOR)-$(VER_RC)" > .tags

docker-cgo-tags:
	@echo "latest-cgo,$(VER_CUT)-cgo,$(VER_MAJOR).$(VER_MINOR)-cgo,$(VER_MAJOR)-cgo" > .tags

docker:
	@docker build -f ./resources/docker/Dockerfile . -t $(DOCKER_IMAGE):$(VER_CUT)
	@docker tag $(DOCKER_IMAGE):$(VER_CUT) $(DOCKER_IMAGE):latest

docker-cgo:
	@docker build -f ./resources/docker/Dockerfile.cgo . -t $(DOCKER_IMAGE):$(VER_CUT)-cgo
	@docker tag $(DOCKER_IMAGE):$(VER_CUT)-cgo $(DOCKER_IMAGE):latest-cgo

fmt:
	@go list -f {{.Dir}} ./... | xargs -I{} gofmt -w -s {}
	@go list -f {{.Dir}} ./... | xargs -I{} goimports -w -local github.com/benthosdev/benthos/v4 {}
	@go mod tidy

lint:
	@go vet $(GO_FLAGS) ./...
	@golangci-lint -j $(GOMAXPROCS) run --timeout 5m cmd/... internal/... public/...

test: $(APPS)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -timeout 3m ./...
	@$(PATHINSTBIN)/benthos template lint ./template/...
	@$(PATHINSTBIN)/benthos test ./config/test/...

test-race: $(APPS)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -timeout 3m -race ./...

test-integration:
	$(warning WARNING! Running the integration tests in their entirety consumes a huge amount of computing resources and is likely to time out on most machines. It's recommended that you instead run the integration suite for connectors you are working selectively with `go test -run 'TestIntegration/kafka' ./...` and so on.)
	@go test $(GO_FLAGS) -ldflags "$(LD_FLAGS)" -run "^Test.*Integration.*$$" -timeout 5m ./...

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist
	rm -rf $(DEST_DIR)/tools
	rm -rf $(DEST_DIR)/serverless
	rm -rf $(PATHINSTDOCKER)

docs: $(APPS) $(TOOLS)
	@$(PATHINSTTOOLS)/benthos_docs_gen $(DOCS_FLAGS)
	@$(PATHINSTBIN)/benthos lint --deprecated "./config/**/*.yaml" \
		"$(WEBSITE_DIR)/cookbooks/**/*.md" \
		"$(WEBSITE_DIR)/docs/**/*.md"
