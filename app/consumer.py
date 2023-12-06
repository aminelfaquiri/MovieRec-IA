import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch



def create_maping_local() :
    # Define the Elasticsearch server and index name :
    es_server = "http://localhost:9200/"
    index_name = "movierec"

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

# Specify the Elasticsearch Cloud credentials and endpoint :
cloud_id = "fe0889c7219f4abda8c4968ab721e709:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxMzRhYTE1ODE1Y2M0NGJmODQxNmFkNmI2NGEzM2JhZSQwZTYyYWY0Mjk0ZWE0Y2RhODBiMmYxZDZiM2U0OTEwNA=="
api_key = "UVZWTlA0d0JQazllek5fZ3h2WDU6VWZYdHlVOHdTQS1MMXJFLXlQbzI2Zw=="
endpoint_elastic = "https://134aa15815cc44bf8416ad6b64a33bae.us-central1.gcp.cloud.es.io"

def create_maping_cloud(cloud_id,api_key) :
    es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)

    index_name = "movierec"
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
    if not es.indices.exists(index=index_name):
        # Create the index with the specified mapping
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created with mapping.")
    else:
        print(f"Index '{index_name} is already exists")


create_maping_cloud(cloud_id,api_key)


# Create spark session :
spark = SparkSession.builder \
    .appName("MovieRecommender_consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"\
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1,") \
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

# Write the DataFrame to Elasticsearch locale using writeStream
#query = kafka_data.writeStream \
#     .format("org.elasticsearch.spark.sql") \
#     .outputMode("append") \
#     .option("es.resource", "movierec") \
#     .option("es.nodes", "localhost") \
#     .option("es.port", "9200") \
#     .option("checkpointLocation", "./checkpoint/") \


query = kafka_data.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "movierec") \
    .option("es.nodes", endpoint_elastic) \
    .option("es.port", "9243") \
    .option("es.net.http.auth.user", "elastic") \
    .option("es.net.http.auth.pass", 'ii6ujXd91w9XisLrg56ujeNU') \
    .option("es.nodes.wan.only", "true") \
    .option("es.write.operation", "index") \
    .option("checkpointLocation", "./checkpoint/") \

try :
    query.start().awaitTermination()
except Exception as e:
    print(f"Error during Spark job execution: {e}")
