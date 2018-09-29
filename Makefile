.PHONY: all deps rpm docker docker-deps docker-zmq docker-push clean docs test test-race test-integration fmt lint install

TAGS =

INSTALL_DIR    = $(GOPATH)/bin
DEST_DIR       = ./target
PATHINSTBIN    = $(DEST_DIR)/bin
PATHINSTDOCKER = $(DEST_DIR)/docker

VERSION   := $(shell git describe --tags || echo "v0.0.0")
VER_MAJOR := $(shell echo $(VERSION) | cut -f1 -d.)
VER_MINOR := $(shell echo $(VERSION) | cut -f2 -d.)
VER_PATCH := $(shell echo $(VERSION) | cut -f3 -d.)
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

docker:
	@docker build -f ./resources/docker/Dockerfile . -t jeffail/benthos:$(VERSION)
	@docker tag jeffail/benthos:$(VERSION) jeffail/benthos:$(VER_MAJOR)
	@docker tag jeffail/benthos:$(VERSION) jeffail/benthos:$(VER_MAJOR).$(VER_MINOR)
	@docker tag jeffail/benthos:$(VERSION) jeffail/benthos:latest

docker-deps:
	@docker build -f ./resources/docker/Dockerfile --target deps . -t jeffail/benthos:$(VERSION)-deps

docker-zmq:
	@docker build -f ./resources/docker/Dockerfile.zmq . -t jeffail/benthos:$(VERSION)-zmq

docker-push:
	@docker push jeffail/benthos:$(VERSION)-deps; true
	@docker push jeffail/benthos:$(VERSION)-zmq; true
	@docker push jeffail/benthos:$(VERSION); true
	@docker push jeffail/benthos:$(VER_MAJOR); true
	@docker push jeffail/benthos:$(VER_MAJOR).$(VER_MINOR); true
	@docker push jeffail/benthos:latest; true

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
	@go run $(GO_FLAGS) ./cmd/tools/benthos_config_gen/main.go
