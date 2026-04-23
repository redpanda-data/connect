FROM confluentinc/cp-kafka-connect-base:7.7.8
# Install the Confluent JDBC sink connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.14
# Install the MySQL JDBC driver (not bundled with the connector hub package)
RUN curl -fL \
      -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j-8.0.33.jar \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
