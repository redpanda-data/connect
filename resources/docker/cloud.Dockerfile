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
RUN useradd -u 10001 connect

COPY $TARGETPLATFORM/redpanda-connect /tmp/redpanda-connect
RUN setcap 'cap_sys_chroot=+ep' /tmp/redpanda-connect

FROM busybox AS package

LABEL maintainer="Ashley Jeffs <ash.jeffs@redpanda.com>"
LABEL org.opencontainers.image.source="https://github.com/redpanda-data/connect"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /tmp/redpanda-connect /redpanda-connect
COPY config/docker.yaml /connect.yaml

# Pre-create the chroot directory so that volume mounts placed inside it
# (e.g. ConfigMaps at /tmp/chroot/...) don't cause kubelet to create it
# as root-owned, which would prevent the connect user from populating the
# rest of the chroot structure at runtime.
RUN mkdir -p /tmp/chroot && chown 10001:10001 /tmp/chroot

USER connect

EXPOSE 4195

ENTRYPOINT ["/redpanda-connect"]

CMD ["run", "/connect.yaml"]
