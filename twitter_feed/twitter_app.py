import os
import sys
import json
import logging

from dotenv import load_dotenv
import requests
import requests_oauthlib
from kafka import KafkaProducer
from requests_oauthlib import OAuth1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('twitter_app')


def get_kafka_producer() -> KafkaProducer:
    hostname = os.getenv('KAFKA_HOST_NAME')
    port = os.getenv('KAFKA_PORT')
    producer = KafkaProducer(bootstrap_servers=f'{hostname}:{port}')
    return producer


def clean_tweet(line: str) -> bytes:
    full_tweet = json.loads(line)
    tweet_user = full_tweet['user']['screen_name']
    tweet_text = full_tweet['text'].replace("\n", " ")
    tweet_data = f"{tweet_user}:{tweet_text}".encode('utf-8')
    logger.info(f"Tweet Text: {tweet_data}")
    return tweet_data


def send_tweets_to_kafka(http_resp: requests.models.Response):
    producer = get_kafka_producer()
    topic = os.getenv('KAFKA_TWEETS_TOPIC')
    for line in http_resp.iter_lines():
        try:
            tweet = clean_tweet(line)
            producer.send(topic, tweet)
        except Exception as e:
            logger.error(f"Kafka write fail: {e}")
            http_resp.close()
            sys.exit(-1)


def tweets_stream() -> requests.models.Response:
    location = os.getenv('TWIT_STREAM_LOCATION')
    payload = {
        'language': 'en',
        'locations': location,
        'track': '#'
    }
    try:
        url = os.getenv('TWIT_STREAM_URL')
        resp = requests.get(url, auth=oauth(), stream=True,
                            params=payload)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logger.error('Failed to connect to twitter')
        logger.error(e)
        raise SystemExit
    except requests.exceptions.HTTPError as e:
        logger.error('Bad response from twitter')
        logger.error(e)
        raise SystemExit
    return resp


def oauth() -> OAuth1:
    consumer_key = os.getenv('TWIT_CONSUMER_KEY')
    consumer_secret = os.getenv('TWIT_CONSUMER_SECRET')
    access_token = os.getenv('TWIT_ACCESS_TOKEN')
    access_secret = os.getenv('TWIT_ACCESS_SECRET')

    oauth_val = requests_oauthlib.OAuth1(consumer_key, consumer_secret,
                                         access_token, access_secret)
    return oauth_val


def main():
    ts = tweets_stream()
    send_tweets_to_kafka(ts)


if __name__ == "__main__":
    load_dotenv(override=True)
    main()
