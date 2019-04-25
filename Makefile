.PHONY: all deps rpm docker docker-deps docker-cgo docker-push clean docs test test-race test-integration fmt lint install

TAGS =

INSTALL_DIR    = $(GOPATH)/bin
DEST_DIR       = ./target
PATHINSTBIN    = $(DEST_DIR)/bin
PATHINSTSERVERLESS = $(DEST_DIR)/serverless
PATHINSTDOCKER = $(DEST_DIR)/docker

VERSION   := $(shell git describe --tags || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)
VER_MAJOR := $(shell echo $(VER_CUT) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VER_CUT) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VER_CUT) | cut -f3 -d.)
DATE      := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

VER_FLAGS = -X main.Version=$(VERSION) \
	-X main.DateBuilt=$(DATE)

LD_FLAGS =
GO_FLAGS =

APPS = benthos
all: $(APPS)

install: $(APPS)
	@cp $(PATHINSTBIN)/* $(INSTALL_DIR)/

$(PATHINSTBIN)/%: $(wildcard lib/*/*.go lib/*/*/*.go lib/*/*/*/*.go cmd/*/*.go)
	@mkdir -p $(dir $@)
	@go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

SERVERLESS = benthos-lambda
serverless: $(SERVERLESS)

$(PATHINSTSERVERLESS)/%: $(wildcard lib/*/*.go lib/*/*/*.go lib/*/*/*/*.go cmd/serverless/*/*.go)
	@mkdir -p $(dir $@)
	@GOOS=linux go build $(GO_FLAGS) -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/serverless/$*
	@zip -m -j $@.zip $@

$(SERVERLESS): %: $(PATHINSTSERVERLESS)/%

docker-tags:
	@echo "latest,$(VER_CUT),v$(VER_MAJOR).$(VER_MINOR),$(VER_MAJOR).$(VER_MINOR),v$(VER_MAJOR),$(VER_MAJOR)" > .tags

docker-cgo-tags:
	@echo "latest-cgo,$(VER_CUT)-cgo,v$(VER_MAJOR).$(VER_MINOR)-cgo,$(VER_MAJOR).$(VER_MINOR)-cgo,v$(VER_MAJOR)-cgo,$(VER_MAJOR)-cgo" > .tags

docker:
	@docker build -f ./resources/docker/Dockerfile . -t jeffail/benthos:$(VER_CUT)
	@docker tag jeffail/benthos:$(VER_CUT) jeffail/benthos:latest

docker-deps:
	@docker build -f ./resources/docker/Dockerfile --target deps . -t jeffail/benthos:$(VER_CUT)-deps
	@docker tag jeffail/benthos:$(VER_CUT)-deps jeffail/benthos:latest-deps

docker-cgo:
	@docker build -f ./resources/docker/Dockerfile.cgo . -t jeffail/benthos:$(VER_CUT)-cgo
	@docker tag jeffail/benthos:$(VER_CUT)-cgo jeffail/benthos:latest-cgo

fmt:
	@go list -f {{.Dir}} ./... | xargs -I{} gofmt -w -s {}

lint:
	@go vet $(GO_FLAGS) ./...
	@golint -min_confidence 0.5 ./cmd/... ./lib/...

test:
	@go test $(GO_FLAGS) -short ./...

test-race:
	@go test $(GO_FLAGS) -short -race ./...

test-integration:
	@go test $(GO_FLAGS) -timeout 600s ./...

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist
	rm -rf $(DEST_DIR)/serverless
	rm -rf $(PATHINSTDOCKER)

docs: $(APPS)
	@$(PATHINSTBIN)/benthos --print-yaml --all > ./config/everything.yaml; true
	@$(PATHINSTBIN)/benthos --list-inputs > ./docs/inputs/README.md; true
	@$(PATHINSTBIN)/benthos --list-processors > ./docs/processors/README.md; true
	@$(PATHINSTBIN)/benthos --list-conditions > ./docs/conditions/README.md; true
	@$(PATHINSTBIN)/benthos --list-buffers > ./docs/buffers/README.md; true
	@$(PATHINSTBIN)/benthos --list-outputs > ./docs/outputs/README.md; true
	@$(PATHINSTBIN)/benthos --list-caches > ./docs/caches/README.md; true
	@$(PATHINSTBIN)/benthos --list-rate-limits > ./docs/rate_limits/README.md; true
	@$(PATHINSTBIN)/benthos --list-metrics > ./docs/metrics/README.md; true
	@$(PATHINSTBIN)/benthos --list-tracers > ./docs/tracers/README.md; true
	@go run $(GO_FLAGS) ./cmd/tools/benthos_config_gen/main.go
