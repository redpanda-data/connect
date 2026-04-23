FROM confluentinc/cp-kafka-connect-base:7.7.8
RUN curl -fL \
      -o /tmp/iceberg-kafka-connect.zip \
      "https://github.com/databricks/iceberg-kafka-connect/releases/download/v0.6.19/iceberg-kafka-connect-runtime-0.6.19.zip" && \
    confluent-hub install --no-prompt \
      --component-dir /usr/share/confluent-hub-components/ \
      /tmp/iceberg-kafka-connect.zip && \
    rm /tmp/iceberg-kafka-connect.zip
