FROM golang:1.21 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN useradd -u 10001 benthos

WORKDIR /go/src/github.com/benthosdev/benthos/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/benthosdev/benthos/
RUN go mod download

# Build
COPY . /go/src/github.com/benthosdev/benthos/
# Tag timetzdata required for busybox base image:
# https://github.com/benthosdev/benthos/issues/897
RUN make TAGS="timetzdata"

# Pack
FROM busybox AS package

LABEL maintainer="Ashley Jeffs <ash@jeffail.uk>"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/benthosdev/benthos/target/bin/benthos .
COPY ./config/docker.yaml /benthos.yaml

USER benthos

EXPOSE 4195

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml"]
