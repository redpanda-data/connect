FROM mcr.microsoft.com/vscode/devcontainers/go:1.21

ARG NODE_VERSION="16"
ARG GOLANGCI_LINT_VERSION="1.57.1"

# https://github.com/microsoft/vscode-dev-containers/blob/main/containers/go/.devcontainer/base.Dockerfile
ENV USERNAME=vscode
ENV LIBRARY_SCRIPTS_SRC="https://raw.githubusercontent.com/microsoft/vscode-dev-containers/main/containers/go/.devcontainer/library-scripts/node-debian.sh"
ENV NVM_DIR=/usr/local/share/nvm
ENV NVM_SYMLINK_CURRENT=true \
  PATH=${NVM_DIR}/current/bin:${PATH}
RUN mkdir /tmp/library-scripts \
  && curl -fsSL -o /tmp/library-scripts/node-debian.sh "${LIBRARY_SCRIPTS_SRC}"
RUN bash /tmp/library-scripts/node-debian.sh "${NVM_DIR}" "${NODE_VERSION}" "${USERNAME}" \
  && apt-get clean -y && rm -rf /var/lib/apt/lists/* \
  && rm -rf /tmp/library-scripts

RUN echo 'deb [trusted=yes] https://repo.goreleaser.com/apt/ /' \
  | tee /etc/apt/sources.list.d/goreleaser.list
RUN apt-get update \
  && export DEBIAN_FRONTEND=noninteractive \
  && apt-get -y install --no-install-recommends goreleaser

RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh \
  | sh -s -- -b $(go env GOPATH)/bin v$GOLANGCI_LINT_VERSION

USER vscode
WORKDIR /home/vscode

RUN mkdir -p .config/git \
  && echo ".vscode/*" >> .config/git/ignore \
  && echo "*.code-workspace" >> .config/git/ignore \
  && echo ".history/" >> .config/git/ignore
