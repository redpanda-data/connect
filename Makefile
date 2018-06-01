.PHONY: all deps rpm docker clean docs test fmt lint install docker-export

BENTHOS_PATH = github.com/Jeffail/benthos

TAGS =

INSTALL_DIR    = $(GOPATH)/bin
DEST_DIR       = ./target
PATHINSTBIN    = $(DEST_DIR)/bin
PATHINSTDOCKER = $(DEST_DIR)/docker

VERSION := $(shell git describe --tags || echo "v0.0.0")
DATE    := $(shell date +"%c" | tr ' :' '__')

VER_FLAGS = -X $(BENTHOS_PATH)/lib/util/service.Version=$(VERSION) \
	-X $(BENTHOS_PATH)/lib/util/service.DateBuilt=$(DATE)

LD_FLAGS =

APPS = benthos
all: $(APPS)

$(PATHINSTBIN)/benthos: $(wildcard lib/*/*.go lib/*/*/*.go lib/*/*/*/*.go cmd/benthos/*.go)

install: $(PATHINSTBIN)/benthos
	@cp $(PATHINSTBIN)/benthos $(INSTALL_DIR)/benthos

$(PATHINSTBIN)/%:
	@mkdir -p $(dir $@)
	@go build -tags "$(TAGS)" -ldflags "$(LD_FLAGS) $(VER_FLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

$(PATHINSTDOCKER)/benthos.tar: docker
	@mkdir -p $(dir $@)
	@docker save jeffail/benthos:$(VERSION) > $@

docker-export: $(PATHINSTDOCKER)/benthos.tar

docker:
	@docker rmi jeffail/benthos:$(VERSION); true
	@docker build -f ./resources/docker/Dockerfile . -t jeffail/benthos:$(VERSION)
	@docker rmi jeffail/benthos:latest; true
	@docker tag jeffail/benthos:$(VERSION) jeffail/benthos:latest

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

test-integration:
	@go test -timeout 60s ./...

rpm:
	@rpmbuild --define "_version $(VERSION)" -bb ./resources/rpm/benthos.spec

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(PATHINSTDOCKER)

docs: $(APPS)
	@$(PATHINSTBIN)/benthos --print-yaml > ./config/everything.yaml; true
	@$(PATHINSTBIN)/benthos --list-inputs > ./docs/inputs/README.md; true
	@$(PATHINSTBIN)/benthos --list-processors > ./docs/processors/README.md; true
	@$(PATHINSTBIN)/benthos --list-conditions > ./docs/conditions/README.md; true
	@$(PATHINSTBIN)/benthos --list-buffers > ./docs/buffers/README.md; true
	@$(PATHINSTBIN)/benthos --list-outputs > ./docs/outputs/README.md; true
	@$(PATHINSTBIN)/benthos --list-caches > ./docs/caches/README.md; true
	@go run ./cmd/tools/benthos_config_gen/main.go
