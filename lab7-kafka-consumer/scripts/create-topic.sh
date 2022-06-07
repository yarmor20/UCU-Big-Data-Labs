docker run -it --rm \
  --network kafka-network \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \
  bitnami/kafka:latest kafka-topics.sh --create \
  --bootstrap-server kafka-server:9093 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tweets