# TwitStream
Realtime hashtag analytics

### Setup
Get api keys  
`https://developer.twitter.com/en/portal/dashboard`

Create and add to .env file in project root folder
```
PROJECT_NAME = 'hashtag_aggregator'

KAFKA_HOST_NAME = 'localhost'
KAFKA_PORT = 9092
KAFKA_TWEETS_TOPIC = 'tweets'
KAFKA_HASHTAG_TOPIC = 'hashtagcount'

SPARK_CHECKPOINT_DIR = 'xxxx'

TWIT_ACCESS_TOKEN = 'xxxx'
TWIT_ACCESS_SECRET = 'xxxx'
TWIT_CONSUMER_KEY = 'xxxx'
TWIT_CONSUMER_SECRET = 'xxxx'

TWIT_STREAM_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
TWIT_STREAM_LOCATION = '-2.768555,53.121229,-2.305756,53.353010'

WEBSERVER_HOSTNAME = '0.0.0.0'
WEBSERVER_PORT = 8000
```
### 1. Start kafka container
`docker-compose up -d`

### 2. Start twitter feed
`python ./twitter_feed/twitter_app.py`

### 3. Start spark stream app
`python ./spark_stream/spark_stream_app.py`

### 4. Start web server
`uvicorn main:app --reload`

Server on http://localhost:8000/  

Documentation on http://127.0.0.1:8000/docs#/  
TODO: add the consumer message to the docs

### Dev notes
- You need a spark installation with the kafka jars installed (see todo)
- There are 2 scripts in the dev directory for testing that feeds/consumers are working.

### Notes links 
https://iwpnd.pw/articles/2020-03/apache-kafka-fastapi-geostream  

Listen for twitter stream  
`telnet localhost 9009`

Find a bounding box  
http://bboxfinder.com/

### TODO
- Lots
- Move feeder to aiokafka
- Use spark docker container