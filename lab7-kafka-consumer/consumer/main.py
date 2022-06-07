from aiokafka import AIOKafkaConsumer
import csv
import json
import asyncio


KAFKA_BROKER = "kafka-server:9093"
KAFKA_TOPIC = "tweets"
CONSUMER_GROUP = "tweet-consumer-group"


# Create event loop for asynchronous kafka producer.
kafka_loop = asyncio.get_event_loop()


async def main():
    # Initialize a Kafka consumer.
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        loop=kafka_loop,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP
    )

    # Start consuming.
    await consumer.start()
    try:
        previous_created_at = None
        first_read = True
        tweets = []

        async for record in consumer:
            msg = json.loads(record.value)
            tweet_id, created_at, text = msg["tweet_id"], msg["created_at"], msg["text"]

            # Get rid of seconds.
            created_at = created_at[:-3]

            if first_read:
                previous_created_at = created_at
                tweets.append([tweet_id, created_at, text])
                first_read = False
            elif previous_created_at == created_at and not first_read:
                tweets.append([tweet_id, created_at, text])
            else:
                # Save file.
                created_at_str = created_at.replace("-", "_").replace(" ", "_").replace(":", "_")
                with open(f"./results/tweets_{created_at_str}_00.csv", "w", newline='') as output_file:
                    writer = csv.writer(output_file)
                    [writer.writerow(row) for row in tweets]
                print(f"Successfully writen tweets to [./results/tweets_{created_at_str}_00.csv]")

                # Start all over again.
                previous_created_at = created_at
                tweets = [[tweet_id, created_at, text]]
            # Save message in map and commit to broker that message is recieved.
            await consumer.commit()

    except Exception as err:
        print("Error:", err)
    finally:
        # Stop consuming.
        await consumer.stop()


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
