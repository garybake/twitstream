import findspark
findspark.init()

import logging
import os
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('twitter_stream')


def spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('tweet_stream') \
        .getOrCreate()

    return spark


def kafka_feed(spark: SparkSession) -> DataFrame:
    hostname = os.getenv('KAFKA_HOST_NAME')
    port = os.getenv('KAFKA_PORT')
    topic = os.getenv('KAFKA_TWEETS_TOPIC')

    source = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f'{hostname}:{port}') \
        .option("subscribe", topic) \
        .load()

    return source


def agg_hashtags(df_rows: DataFrame) -> DataFrame:
    df = df_rows\
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("hashtag", F.explode(F.split("value", "\s+"))) \
        .filter(F.col('hashtag').startswith('#')) \
        .groupBy('hashtag') \
        .count() \
        .withColumnRenamed('hashtag', 'key') \
        .withColumn('value', F.col('count').cast("string")) \
        .drop('count')

    return df


def run_agg_stream_kafka(df: DataFrame):
    hostname = os.getenv('KAFKA_HOST_NAME')
    port = os.getenv('KAFKA_PORT')
    topic = os.getenv('KAFKA_HASHTAG_TOPIC')

    checkpoint_dir = os.path.join(get_checkpoints_dir(), 'hashtagscheckpoint')

    qry = df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f'{hostname}:{port}') \
        .option("topic", topic) \
        .option('checkpointLocation', checkpoint_dir)\
        .outputMode('complete')\
        .start()

    qry.awaitTermination()


def get_checkpoints_dir():
    chk_path = os.getenv('SPARK_CHECKPOINT_DIR')
    if not os.path.isdir(chk_path):
        print(f'Failed to find {chk_path}')
        sys.exit(-1)
    return chk_path


def main():
    spark = spark_session()
    source = kafka_feed(spark)
    agg_stream = agg_hashtags(source)
    run_agg_stream_kafka(agg_stream)


if __name__ == "__main__":
    load_dotenv(override=True)
    main()
