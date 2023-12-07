import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime,date_format,col,from_json
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType,DateType
from elasticsearch import Elasticsearch
from  info_sensetive import cloud_id,api_key,endpoint_elastic,password


def create_maping_local() :
    # Define the Elasticsearch server and index name :
    es_server = "http://localhost:9200/"
    index_name = "movierec_index"

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
    try :
        # Create the index with the specified mapping :
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            print(f"Index '{index_name}' created with mapping.")

        else:
            es.indices.delete(index=index_name)
            es.indices.create(index=index_name)
            print(f"Index '{index_name} is already exists")

    except Exception as e:
            print(f"Error Creation index: {e} ")

# create_maping_local()

def create_maping_cloud(cloud_id,api_key) :
    try :
        es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)

    except Exception as e:
        print(f"Error Elastecsearch Connection: {e}")



    index_name = "movierec_index"
    mapping = {
        "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "age": {"type": "integer"},
                    "gender": {"type": "keyword"},
                    "movie": {
                        "properties": {
                            "genres": {"type": "keyword"},
                            "movieId": {"type": "integer"},
                            "title": {"type": "keyword"}
                        }
                    },
                    "rating": {"type": "integer"},
                    "timestamp": {"type": "date"},
                    "userId": {"type": "integer"}
                }
        }
    }

    try :
        # Create the index with the specified mapping :
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            print(f"Index '{index_name}' created with mapping.")

        else:
            es.indices.delete(index=index_name)
            es.indices.create(index=index_name, body=mapping)
            print(f"Index '{index_name} is already exists")

    except Exception as e:
            print(f"Error Creation index: {e} ")

create_maping_cloud(cloud_id,api_key)

# Create spark session :
spark = SparkSession.builder \
    .appName("MovieRecommender_consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"\
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,") \
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
    StructField("id", StringType()),
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
kafka_data = kafka_data.select(from_json(col("value"),schema=schema).alias("data")).select("data.*")\

kafka_data = kafka_data.withColumn(
    "timestamp",
    date_format(from_unixtime(col("timestamp").cast("double")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

# Print the schema of my Data :
kafka_data.printSchema()

# Write the DataFrame to Elasticsearch locale :
# query = kafka_data.writeStream \
#     .format("org.elasticsearch.spark.sql") \
#     .outputMode("append") \
#     .option("es.resource", "movierec_index") \
#     .option("es.nodes", "localhost") \
#     .option("es.port", "9200") \
#     .option("checkpointLocation", "./checkpoint/") \

#  Write the DataFrame to Elasticsearch cloud  :
query = kafka_data.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "movierec_index") \
    .option("es.nodes", endpoint_elastic) \
    .option("es.port", "9243") \
    .option("es.net.http.auth.user", "elastic") \
    .option("es.net.http.auth.pass", password) \
    .option("es.nodes.wan.only", "true") \
    .option("es.mapping.id", "id") \
    .option("es.write.operation", "index") \
    .option("checkpointLocation", "./checkpoint/")

try :
    query.start().awaitTermination()
except Exception as e:
    print(f"Error during Spark job execution: {e}")