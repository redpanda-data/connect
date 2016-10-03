FROM golang:latest

MAINTAINER Ashley Jeffs <ash.jeffs@gmail.com>

RUN apt-get update && apt-get install -y pkg-config
RUN /bin/bash -c 'cd /tmp; \
	curl -s "https://download.libsodium.org/libsodium/releases/LATEST.tar.gz" | tar -xz; \
	( cd ./libsodium-* && ./configure && make && make install ); \
	curl -sL "https://archive.org/download/zeromq_4.1.4/zeromq-4.1.4.tar.gz" | tar -xz; \
	( cd ./zeromq-4.1.4 && ./configure && make && make install ); \
	ldconfig;'
RUN go get -tags "ZMQ4" github.com/jeffail/benthos/cmd/benthos
