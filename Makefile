.PHONY: all serverless deps docker docker-cgo clean docs test test-race test-integration fmt lint install deploy-docs

TAGS =

INSTALL_DIR        = $(GOPATH)/bin
DEST_DIR           = ./target
PATHINSTBIN        = $(DEST_DIR)/bin
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

lint:
	@go vet $(GO_FLAGS) ./...
	@golint -min_confidence 0.5 ./cmd/... ./lib/...

test:
	@go test $(GO_FLAGS) -timeout 300s -short ./...

test-race:
	@go test $(GO_FLAGS) -timeout 300s -short -race ./...

test-integration:
	@go test $(GO_FLAGS) -timeout 600s ./...

clean:
	rm -rf $(PATHINSTBIN)
	rm -rf $(DEST_DIR)/dist
	rm -rf $(DEST_DIR)/serverless
	rm -rf $(PATHINSTDOCKER)

docs: $(APPS)
	@go run $(GO_FLAGS) ./cmd/tools/benthos_config_gen/main.go
	@go run $(GO_FLAGS) ./cmd/tools/benthos_docs_gen/main.go

deploy-docs:
	@git diff-index --quiet HEAD -- || ( echo "Failed: Branch must be clean"; false )
	@mkdocs build -f ./.mkdocs.yml
	@git fetch origin gh-pages
	@git checkout gh-pages ./archive
	@git reset HEAD
	@mv ./archive ./site/
	@git checkout gh-pages
	@ls -1 | grep -v "site" | xargs rm -rf
	@mv site/* .
	@rmdir ./site
	@git add -A
	@git commit -m 'Deployed ${VERSION} with MkDocs'
	@git push origin gh-pages
	@git checkout master
