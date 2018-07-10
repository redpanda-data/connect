.PHONY: all deps rpm docker clean docs test test-race test-integration fmt lint install docker-zmq

TAGS =

INSTALL_DIR    = $(GOPATH)/bin
DEST_DIR       = ./target
PATHINSTBIN    = $(DEST_DIR)/bin
PATHINSTDOCKER = $(DEST_DIR)/docker

VERSION := $(shell git describe --tags || echo "v0.0.0")
DATE    := $(shell date +"%Y-%m-%dT%H:%M:%SZ")

VER_FLAGS = -X main.Version=$(VERSION) \
	-X main.DateBuilt=$(DATE)

LD_FLAGS =

APPS = benthos
all: $(APPS)

install: $(APPS)
	@cp $(PATHINSTBIN)/* $(INSTALL_DIR)/

$(PATHINSTBIN)/%: $(wildcard lib/*/*.go lib/*/*/*.go lib/*/*/*/*.go cmd/*/*.go)
	@mkdir -p $(dir $@)
	@go build -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

docker:
	@docker rmi jeffail/benthos:$(VERSION); true
	@docker build -f ./resources/docker/Dockerfile . -t jeffail/benthos:$(VERSION)
	@docker rmi jeffail/benthos:latest; true
	@docker tag jeffail/benthos:$(VERSION) jeffail/benthos:latest

docker-zmq:
	@docker rmi jeffail/benthos:$(VERSION)-zmq; true
	@docker build -f ./resources/docker/Dockerfile.zmq . -t jeffail/benthos:$(VERSION)-zmq

deps:
	@go get github.com/golang/dep/cmd/dep
	@$$GOPATH/bin/dep ensure

fmt:
	@go list ./... | xargs -I{} gofmt -w -s $$GOPATH/src/{}

lint:
	@go vet ./...
	@golint ./cmd/... ./lib/...

test:
	@go test -short ./...

test-race:
	@go test -short -race ./...

test-integration:
	@go test -timeout 300s ./...

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
	@go run ./cmd/tools/benthos_config_gen/main.go
