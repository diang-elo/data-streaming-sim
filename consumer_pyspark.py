import findspark
findspark.init() 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Normalizer, StandardScaler



#topic name to match same topic name in simulator_v2.py
kafka_topic_name = "building-info"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("Structured Streaming") \
        .master("local[*]") \
        .getOrCreate()


# Construct a streaming DataFrame that reads from topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "./V2") \
    .start()

query.awaitTermination()