"""Something to test data being read off the queue"""
import os

from dotenv import load_dotenv

load_dotenv(override=True)

print('starting to read data')
topic = os.getenv('KAFKA_HASHTAG_TOPIC')
hostname = os.getenv('KAFKA_HOST_NAME')
port = os.getenv('KAFKA_PORT')
bootstrap_servers = f'{hostname}:{port}'
print(bootstrap_servers)

from aiokafka import AIOKafkaConsumer
import asyncio


async def consume():
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers)

    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

asyncio.run(consume())
