"""Something to feed test data to the queue"""

import random
import time
import sys
import os

from dotenv import load_dotenv
from kafka import KafkaProducer


def main():
    hostname = os.getenv('KAFKA_HOST_NAME')
    port = os.getenv('KAFKA_PORT')
    producer = KafkaProducer(bootstrap_servers=f'{hostname}:{port}')
    topic = os.getenv('KAFKA_TWEETS_TOPIC')
    # topic = os.getenv('KAFKA_HASHTAG_TOPIC')

    while True:
        try:
            a_number = random.randint(0, 20)
            msg = 'some bytes #{}'.format(a_number).encode('utf-8')
            if random.random() > 0.9:
                msg = 'some bytes #{} more bytes #{}'.format(a_number, random.randint(0, 20)).encode('utf-8')
            print(msg)
            producer.send(topic, msg)
            time.sleep(3)
        except KeyboardInterrupt:
            producer.close()
            print('Closed connection')
            sys.exit()


if __name__ == "__main__":
    load_dotenv(override=True)
    main()
