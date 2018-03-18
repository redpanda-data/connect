.PHONY: all deps rpm docker clean-docker clean docs test fmt

BENTHOS_PATH = github.com/Jeffail/benthos

TAGS =

DEST_DIR       = ./target
PATHINSTBIN    = $(DEST_DIR)/bin
PATHINSTDOCKER = $(DEST_DIR)/docker

VERSION := $(shell git describe --tags || echo "v0.0.0")
DATE    := $(shell date +"%c" | tr ' :' '__')

LDFLAGS = -X $(BENTHOS_PATH)/lib/util/service.Version=$(VERSION) \
	-X $(BENTHOS_PATH)/lib/util/service.DateBuilt=$(DATE)

APPS = benthos
all: $(APPS)

$(PATHINSTBIN)/benthos: $(wildcard lib/*/*.go lib/*/*/*.go lib/*/*/*/*.go cmd/benthos/*.go)

$(PATHINSTBIN)/%: deps
	@mkdir -p $(dir $@)
	@go build -tags "$(TAGS)" -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

$(PATHINSTDOCKER)/benthos.tar:
	@mkdir -p $(dir $@)
	@docker build -f ./resources/docker/Dockerfile . -t benthos:$(VERSION)
	@docker tag benthos:$(VERSION) benthos:latest
	@docker tag benthos:latest jeffail/benthos:latest
	@docker save benthos:$(VERSION) > $@

docker: $(PATHINSTDOCKER)/benthos.tar

deps:
	@go get github.com/golang/dep/cmd/dep
	@$$GOPATH/bin/dep ensure

fmt:
	@go list ./... | xargs -I{} gofmt -w -s $$GOPATH/src/{}

test:
	@go test ./...

test-integration:
	@go test -tags "integration" -timeout 20s ./...

rpm:
	@rpmbuild --define "_version $(VERSION)" -bb ./resources/rpm/benthos.spec

clean:
	rm -rf $(PATHINSTBIN)

clean-docker:
	rm -rf $(PATHINSTDOCKER)
	docker rmi jeffail/benthos; true
	docker rmi benthos:latest; true
	docker rmi benthos:$(VERSION); true

docs: $(APPS)
	@$(PATHINSTBIN)/benthos --print-yaml > ./config/everything.yaml; true
	@$(PATHINSTBIN)/benthos --list-inputs > ./resources/docs/inputs/README.md; true
	@$(PATHINSTBIN)/benthos --list-processors > ./resources/docs/processors/README.md; true
	@$(PATHINSTBIN)/benthos --list-conditions > ./resources/docs/conditions/README.md; true
	@$(PATHINSTBIN)/benthos --list-buffers > ./resources/docs/buffers/README.md; true
	@$(PATHINSTBIN)/benthos --list-outputs > ./resources/docs/outputs/README.md; true
	@go run ./cmd/tools/benthos_config_gen/main.go
