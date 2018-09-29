FROM golang:latest AS deps

WORKDIR /go/src/github.com/Jeffail/benthos/
COPY go.mod go.sum /go/src/github.com/Jeffail/benthos/

ENV GO111MODULE on
RUN go mod download

FROM golang:latest AS build

RUN useradd -u 10001 benthos

COPY --from=deps /go/pkg /go/pkg

WORKDIR /go/src/github.com/Jeffail/benthos/
COPY . /go/src/github.com/Jeffail/benthos/

ENV GO111MODULE on
RUN CGO_ENABLED=0 GOOS=linux make

FROM scratch AS package

LABEL maintainer="Ashley Jeffs <ash.jeffs@gmail.com>"

WORKDIR /root/

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/Jeffail/benthos/target/bin/benthos .
COPY ./config/env/default.yaml /benthos.yaml

USER benthos

EXPOSE 4195

ENTRYPOINT ["./benthos", "-c", "/benthos.yaml"]
