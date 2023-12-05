import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch


# Create spark session :
spark = SparkSession.builder \
    .appName("MovieRecommender_consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"\
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .getOrCreate()

# Read data from Kafka :
kafka_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "MovieRecommender") \
    .load()

print("____________________________data is loaded _____________________")
# Specify the value deserializer :
kafka_data = kafka_data.selectExpr("CAST(value AS STRING)")

print("____________________________ Casting data _____________________")

# Define the schema
schema = StructType([
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("movie", StructType([
        StructField("genres", ArrayType(StringType())),
        StructField("movieId", IntegerType()),
        StructField("title", StringType())
    ])),
    StructField("rating", IntegerType()),
    StructField("timestamp", IntegerType()),
    StructField("userId", IntegerType())
])


# change parce to json :
kafka_data = kafka_data.select(from_json(col("value"),schema=schema).alias("data"))\


kafka_data.printSchema()

kafka_data.writeStream.outputMode("append").format("console").start().awaitTermination()
