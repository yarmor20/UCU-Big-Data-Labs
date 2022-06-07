from src.__kafka_producer import TweetProducer
from datetime import datetime
import asyncio
import time
import csv


KAFKA_BROKER = "kafka-server:9093"
KAFKA_TOPIC = "tweets"


async def main():
    producer = TweetProducer(logger_name="TweetProducer", broker=KAFKA_BROKER)
    await producer.start()

    with open("./data/twcs.csv", "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # Start sending messages to Kafka topic one by one.
        for line in reader:
            # Prepare data for sending.
            message = {
                "tweet_id": line[0],
                "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "text": line[4]
            }

            # Send the message. Throughput has to be 10-15 messages/second.
            await producer.send(topic=KAFKA_TOPIC, message=message)
            time.sleep(0.09)
    await producer.stop()


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
