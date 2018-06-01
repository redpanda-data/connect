FROM golang:latest

WORKDIR /go/src/github.com/Jeffail/benthos/
COPY . /go/src/github.com/Jeffail/benthos/

RUN CGO_ENABLED=0 GOOS=linux make

FROM alpine:latest

LABEL maintainer="Ashley Jeffs <ash.jeffs@gmail.com>"

WORKDIR /root/

RUN apk --update upgrade
RUN apk add ca-certificates
RUN update-ca-certificates

COPY --from=0 /go/src/github.com/Jeffail/benthos/target/bin/benthos .
COPY ./config/env/default.yaml /benthos.yaml

EXPOSE 4195

ENTRYPOINT ["./benthos", "-c", "/benthos.yaml"]
