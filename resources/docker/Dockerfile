FROM golang:latest

WORKDIR /go/src/github.com/Jeffail/benthos/
COPY . /go/src/github.com/Jeffail/benthos/

RUN CGO_ENABLED=0 GOOS=linux make

FROM scratch

LABEL maintainer="Ashley Jeffs <ash.jeffs@gmail.com>"

WORKDIR /root/

COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=0 /go/src/github.com/Jeffail/benthos/target/bin/benthos .
COPY ./config/env/default.yaml /benthos.yaml

EXPOSE 4195

ENTRYPOINT ["./benthos", "-c", "/benthos.yaml"]
