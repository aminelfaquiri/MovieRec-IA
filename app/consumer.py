import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch



# Define the Elasticsearch server and index name :
es_server = "http://localhost:9200/"
index_name = "bb"

try :
    # Create an Elasticsearch client :
    es = Elasticsearch([es_server])
    print("coonection is succefulty")
except :
    print("connection problem")
# Define the mapping for index :
mapping = {
    "mappings": {
            "properties": {
                "age": {"type": "integer"},
                "gender": {"type": "keyword"},
                "movie": {
                    "properties": {
                        "genres": {"type": "keyword"},
                        "movieId": {"type": "integer"},
                        "title": {"type": "text"}
                    }
                },
                "rating": {"type": "integer"},
                "timestamp": {"type": "date"},
                "userId": {"type": "integer"}
            }
    }
}

# Create the index with the specified mapping :
es.indices.create(index=index_name, body=mapping)

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

# Specify the value deserializer :
kafka_data = kafka_data.selectExpr("CAST(value AS STRING)")

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
kafka_data = kafka_data.select(from_json(col("value"),schema=schema).alias("data")).select("data.*")

# Print the schema of my Data :
kafka_data.printSchema()


# Write the DataFrame to Elasticsearch using writeStream
kafka_data.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "bb") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("checkpointLocation", "./checkpoint/") \
    .start().awaitTermination()