# Create a new topic.
docker run -it --rm \
    --network kafka-network \
    -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \
    bitnami/kafka:latest kafka-topics.sh --create  \
    --bootstrap-server kafka-server:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic test-topic

# Show the list of topics.
docker run -it --rm \
    --network kafka-network \
    -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \
    bitnami/kafka:latest kafka-topics.sh --list  \
    --bootstrap-server kafka-server:9092

# Start Kakfa Producer.
docker run -it --rm \
    --network kafka-network \
    -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \
    bitnami/kafka:latest kafka-console-producer.sh \
    --broker-list kafka-server:9092 \
    --topic test-topic

# Start Kakfa Consumer.
docker run -it --rm \
    --network kafka-network \
    -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \
    bitnami/kafka:latest kafka-console-consumer.sh \
    --bootstrap-server kafka-server:9092 \
    --topic test-topic