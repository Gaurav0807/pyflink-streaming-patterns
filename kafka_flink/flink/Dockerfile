FROM flink:1.19.1-scala_2.12-java11

# ---------------------------------------------------------------------------
# PyFlink runtime
# ---------------------------------------------------------------------------
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev python3-venv curl default-jdk-headless build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# pemja (PyFlink's Python<->Java bridge) compiles a native extension against
# JNI headers at install time. The Flink base image's JAVA_HOME points at a
# JRE-only Temurin install with no include/ dir, so point JAVA_HOME at the
# full JDK above just for this step; Flink's own runtime keeps using its
# original JAVA_HOME untouched.
COPY requirements.txt /tmp/requirements.txt
RUN JAVA_HOME=/usr/lib/jvm/default-java pip3 install --no-cache-dir -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# Kafka + Avro/Confluent Schema Registry connector jars for Flink
# ---------------------------------------------------------------------------
ENV FLINK_VERSION=1.19.1
ENV KAFKA_CONNECTOR_VERSION=3.2.0-1.19

RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar && \
    curl -fSL -o /opt/flink/lib/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar && \
    curl -fSL -o /opt/flink/lib/flink-json-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar && \
    curl -fSL -o /opt/flink/lib/flink-parquet-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/${FLINK_VERSION}/flink-parquet-${FLINK_VERSION}.jar

# ---------------------------------------------------------------------------
# S3 FileSystem plugin (for AWS S3 support)
# ---------------------------------------------------------------------------
# Hadoop S3 plugin has recoverable writers, needed for streaming mode with
# checkpointing. Works with AWS S3. Note: Does NOT work with MinIO due to
# multipart semantics differences.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    curl -fSL -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar

RUN mkdir -p /opt/flink/jobs /opt/flink/checkpoints
