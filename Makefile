.PHONY: all deps rpm docker clean-docker clean docs

BENTHOS_PATH = github.com/jeffail/benthos

TAGS =

DEST_DIR       = ./target
PATHINSTBIN    = $(DEST_DIR)/bin
PATHINSTDOCKER = $(DEST_DIR)/docker

VERSION := $(shell git describe --tags || echo "v0.0.0")
DATE    := $(shell date +"%c" | tr ' :' '__')

LDFLAGS = -X $(BENTHOS_PATH)/lib/util/service.version=$(VERSION) \
	-X $(BENTHOS_PATH)/lib/util/service.dateBuilt=$(DATE)

APPS = benthos
all: $(APPS)

$(PATHINSTBIN)/benthos: $(wildcard lib/*/*.go lib/*/*/*.go cmd/benthos/*.go)

$(PATHINSTBIN)/%:
	@mkdir -p $(dir $@)
	@go build -tags "$(TAGS)" -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*

$(APPS): %: $(PATHINSTBIN)/%

$(PATHINSTDOCKER)/benthos.tar: $(PATHINSTBIN)/benthos
	@mkdir -p $(dir $@)
	@docker build -f ./resources/docker/Dockerfile . -t benthos:$(VERSION)
	@docker tag benthos:$(VERSION) benthos:latest
	@docker save benthos:$(VERSION) > $@

docker: $(PATHINSTDOCKER)/benthos.tar

deps:
	@go get -u github.com/golang/dep/cmd/dep
	@dep ensure

rpm:
	@rpmbuild --define "_version $(VERSION)" -bb ./resources/rpm/benthos.spec

clean:
	rm -rf $(PATHINSTBIN)

clean-docker:
	rm -rf $(PATHINSTDOCKER)
	docker rmi benthos:$(VERSION)
	docker rmi benthos

docs: $(APPS)
	@$(PATHINSTBIN)/benthos --list-inputs > ./resources/docs/inputs/list.md; true
	@$(PATHINSTBIN)/benthos --list-processors > ./resources/docs/processors/list.md; true
	@$(PATHINSTBIN)/benthos --list-buffers > ./resources/docs/buffers/list.md; true
	@$(PATHINSTBIN)/benthos --list-outputs > ./resources/docs/outputs/list.md; true
