.PHONY: all serverless deps docker docker-cgo clean docs test test-race test-integration fmt lint install deploy-docs

TAGS =

INSTALL_DIR        = $(GOPATH)/bin
WEBSITE_DIR        = ./website
DEST_DIR           = ./target
PATHINSTBIN        = $(DEST_DIR)/bin
PATHINSTTOOLS      = $(DEST_DIR)/tools
PATHINSTSERVERLESS = $(DEST_DIR)/serverless
PATHINSTDOCKER     = $(DEST_DIR)/docker

VERSION   := $(shell git describe --tags || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)
VER_MAJOR := $(shell echo $(VER_CUT) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VER_CUT) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VER_CUT) | cut -f3 -d.)
VER_RC    := $(shell echo $(VER_PATCH) | cut -f2 -d-)
DATE      := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

VER_FLAGS = -X github.com/Jeffail/benthos/v3/lib/service.Version=$(VERSION) \
	-X github.com/Jeffail/benthos/v3/lib/service.DateBuilt=$(DATE)

LD_FLAGS =
GO_FLAGS =

APPS = benthos
all: $(APPS)

install: $(APPS)
	@cp $(PATHINSTBIN)/* $(INSTALL_DIR)/

deps:
	@go mod tidy
	@go mod vendor

SOURCE_FILES = $(shell find lib internal cmd -type f -name "*.go")

$(PATHINSTBIN)/%: $(SOURCE_FILES)
	@mkdir -p $(dir $@)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

TOOLS = benthos_config_gen benthos_docs_gen
tools: $(TOOLS)

$(PATHINSTTOOLS)/%: $(SOURCE_FILES)
	@mkdir -p $(dir $@)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/tools/$*

$(TOOLS): %: $(PATHINSTTOOLS)/%

SERVERLESS = benthos-lambda
serverless: $(SERVERLESS)

$(PATHINSTSERVERLESS)/%: $(SOURCE_FILES)
	@mkdir -p $(dir $@)
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

docker: deps
	@docker build -f ./resources/docker/Dockerfile . -t jeffail/benthos:$(VER_CUT)
	@docker tag jeffail/benthos:$(VER_CUT) jeffail/benthos:latest

docker-cgo: deps
	@docker build -f ./resources/docker/Dockerfile.cgo . -t jeffail/benthos:$(VER_CUT)-cgo
	@docker tag jeffail/benthos:$(VER_CUT)-cgo jeffail/benthos:latest-cgo

fmt:
	@go list -f {{.Dir}} ./... | xargs -I{} gofmt -w -s {}
	@go mod tidy

lint:
	@go vet $(GO_FLAGS) ./...
	@golint -min_confidence 0.5 ./cmd/... ./lib/...

test: $(APPS)
	@go test $(GO_FLAGS) -timeout 300s -race ./...
	@$(PATHINSTBIN)/benthos test ./config/test/...

test-wasm-build:
	@GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o $(DEST_DIR)/wasm_test ./cmd/benthos

test-race:
	@go test $(GO_FLAGS) -timeout 300s -race ./...

test-integration:
	@go test $(GO_FLAGS) -tags "integration" -timeout 600s ./...

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist
	rm -rf $(DEST_DIR)/tools
	rm -rf $(DEST_DIR)/serverless
	rm -rf $(PATHINSTDOCKER)

docs: $(APPS) $(TOOLS)
	@$(PATHINSTTOOLS)/benthos_config_gen
	@$(PATHINSTTOOLS)/benthos_docs_gen
	@$(PATHINSTBIN)/benthos lint ./config/... \
		$(WEBSITE_DIR)/cookbooks/*.md \
		$(WEBSITE_DIR)/docs/components/**/about.md \
		$(WEBSITE_DIR)/docs/guides/*.md \
		$(WEBSITE_DIR)/docs/guides/**/*.md \
		$(WEBSITE_DIR)/docs/configuration/*.md
