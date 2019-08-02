FROM busybox as download

ARG IDML_VER
ENV IDML_VER ${IDML_VER:-1.0.34}

ARG BENTHOS_VER
ENV BENTHOS_VER ${BENTHOS_VER:-1.15.0}

RUN wget https://github.com/IDML/idml/releases/download/v${IDML_VER}/idml.jar -O ./idml.jar
RUN chmod +x ./idml.jar
RUN wget https://github.com/Jeffail/benthos/releases/download/v${BENTHOS_VER}/benthos_${BENTHOS_VER}_linux_amd64.tar.gz -O ./benthos.tar.gz
RUN tar -xvf ./benthos.tar.gz

RUN adduser -D -H -u 10001 benthos

FROM openjdk:8-jre

LABEL maintainer="Ashley Jeffs <ash@jeffail.uk>"

WORKDIR /

COPY --from=download /etc/passwd /etc/passwd
COPY --from=download ./benthos /benthos
COPY --from=download ./config/env/default.yaml /benthos.yaml
COPY --from=download ./idml.jar /idml.jar

USER benthos

EXPOSE 4195

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml"]
