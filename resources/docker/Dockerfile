FROM golang:1.21 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux

# Prepare the build environment
RUN go env -w GOCACHE=/go-cache; \
    go env -w GOMODCACHE=/gomod-cache; \
    useradd -u 10001 benthos

WORKDIR /go/src/github.com/benthosdev/benthos/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/benthosdev/benthos/
RUN --mount=type=cache,target=/gomod-cache \
    go mod download

# Build
COPY . /go/src/github.com/benthosdev/benthos/
# Tag timetzdata required for busybox base image:
# https://github.com/benthosdev/benthos/issues/897
RUN --mount=type=cache,target=/gomod-cache --mount=type=cache,target=/go-cache \
    make TAGS="timetzdata"

# Pack
FROM busybox AS package

LABEL maintainer="Ashley Jeffs <ash@benthos.dev>"
LABEL org.opencontainers.image.source="https://github.com/benthosdev/benthos"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/benthosdev/benthos/target/bin/benthos .
COPY ./config/docker.yaml /benthos.yaml

USER benthos

EXPOSE 4195

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml"]
