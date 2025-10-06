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

FROM debian:12-slim AS build

RUN apt-get update && apt-get install -y ca-certificates
RUN useradd -u 10001 connect

FROM busybox AS package
ARG TARGETPLATFORM

LABEL maintainer="Ashley Jeffs <ash.jeffs@redpanda.com>"
LABEL org.opencontainers.image.source="https://github.com/redpanda-data/connect"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY $TARGETPLATFORM/redpanda-connect /redpanda-connect
COPY config/docker.yaml /connect.yaml

USER connect

EXPOSE 4195

ENTRYPOINT ["/redpanda-connect"]

CMD ["run", "/connect.yaml"]
