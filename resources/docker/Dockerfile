FROM golang:latest

RUN useradd -u 10001 benthos

WORKDIR /go/src/github.com/Jeffail/benthos/
COPY . /go/src/github.com/Jeffail/benthos/

RUN CGO_ENABLED=0 GOOS=linux make

FROM scratch

LABEL maintainer="Ashley Jeffs <ash.jeffs@gmail.com>"

WORKDIR /root/

COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=0 /etc/passwd /etc/passwd
COPY --from=0 /go/src/github.com/Jeffail/benthos/target/bin/benthos .
COPY ./config/env/default.yaml /benthos.yaml

USER benthos

EXPOSE 4195

ENTRYPOINT ["./benthos", "-c", "/benthos.yaml"]
