docker run -it --rm \
    --network kafka-network \
    -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \
    bitnami/kafka:latest kafka-console-consumer.sh \
    --bootstrap-server kafka-server:9093 \
    --topic tweets