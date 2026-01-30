# Copyright 2024 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

FROM debian:12-slim AS build
ARG TARGETPLATFORM

RUN apt-get update && apt-get install -y ca-certificates libcap2-bin
RUN addgroup --gid 10001 connect
RUN useradd -u 10001 -g connect connect

COPY $TARGETPLATFORM/redpanda-connect /tmp/redpanda-connect
RUN setcap 'cap_sys_chroot=+ep' /tmp/redpanda-connect

RUN touch /tmp/keep

FROM ollama/ollama:latest AS package

# Override the HOST from the ollama dockerfile
ENV OLLAMA_HOST=127.0.0.1

LABEL maintainer="Tyler Rockwood <rockwood@redpanda.com>"
LABEL org.opencontainers.image.source="https://github.com/redpanda-data/connect"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group
COPY --from=build /tmp/redpanda-connect /redpanda-connect
COPY config/docker.yaml /connect.yaml

USER connect

COPY --chown=connect:connect --from=build /tmp/keep /home/connect/.ollama/keep

EXPOSE 4195

ENTRYPOINT ["/redpanda-connect"]

CMD ["run", "/connect.yaml"]
