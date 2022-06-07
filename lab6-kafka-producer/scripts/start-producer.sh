docker run -it \
  --network kafka-network \
  --volume "$(pwd)/../data:/opt/app/data" \
  kafka-producer:1.0