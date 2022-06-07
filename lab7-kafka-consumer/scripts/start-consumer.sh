docker run -it \
  --network kafka-network \
  --volume "$(pwd)/../consumer/results:/opt/app/results" \
  kafka-consumer:1.0