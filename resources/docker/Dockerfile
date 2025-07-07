# Copyright 2024 Redpanda Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.24 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN useradd -u 10001 connect

RUN apt-get update && apt-get install -y libcap2-bin
RUN go install github.com/go-task/task/v3/cmd/task@latest

WORKDIR /go/src/github.com/redpanda-data/connect/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/redpanda-data/connect/
RUN go mod download

# Build
COPY . /go/src/github.com/redpanda-data/connect/
# Tag timetzdata required for busybox base image:
# https://github.com/benthosdev/benthos/issues/897
RUN TAGS="timetzdata" task build:redpanda-connect
RUN setcap 'cap_sys_chroot=+ep' target/redpanda-connect

# Pack
FROM busybox AS package

LABEL maintainer="Ashley Jeffs <ash.jeffs@redpanda.com>"
LABEL org.opencontainers.image.source="https://github.com/redpanda-data/connect"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/redpanda-data/connect/target/redpanda-connect .
COPY ./config/docker.yaml /connect.yaml

USER connect

EXPOSE 4195

ENTRYPOINT ["/redpanda-connect"]

CMD ["run", "/connect.yaml"]
