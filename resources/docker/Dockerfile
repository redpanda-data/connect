FROM golang:1.13 AS build

RUN useradd -u 10001 benthos

WORKDIR /go/src/github.com/Jeffail/benthos/
COPY . /go/src/github.com/Jeffail/benthos/

ENV GO111MODULE on
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=vendor make

FROM busybox AS package

LABEL maintainer="Ashley Jeffs <ash@jeffail.uk>"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/Jeffail/benthos/target/bin/benthos .
COPY ./config/env/default.yaml /benthos.yaml

USER benthos

EXPOSE 4195

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml"]
